package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// wakeMode decides what the watcher does when a sleeping VCI has pending
// GitOps work.
type wakeMode string

const (
	// wakeModeOn wakes the VCI and leaves it to the normal sleep timer. The
	// empty value means the same, so a zero watcherConfig keeps the original
	// behavior.
	wakeModeOn wakeMode = "true"
	// wakeModeOff never wakes the VCI. Its pending work waits until something
	// else wakes it.
	wakeModeOff wakeMode = "false"
	// wakeModeSync wakes the VCI, waits for the deploy to finish, and puts it
	// straight back to sleep unless someone else used it in the meantime.
	wakeModeSync wakeMode = "sync"

	defaultSleepAfterSyncTimeout = 15 * time.Minute
	defaultSleepAfterSyncSettle  = time.Minute

	// sleepModeForceAnnotation makes vCluster Platform put the VCI to sleep,
	// the same as `vcluster platform sleep`. Platform clears it on new activity.
	sleepModeForceAnnotation  = "sleepmode.loft.sh/force"
	sleepModeLastActivityAnno = "sleepmode.loft.sh/last-activity"
	// sleepModeLastActivityInfoAnno holds Platform's JSON LastActivityInfo;
	// its subject names who caused the last activity.
	sleepModeLastActivityInfoAnno = "sleepmode.loft.sh/last-activity-info"

	// otherActivitySlackSeconds absorbs Platform's own timestamp updates when
	// the wake subject is unknown and activity is compared by time only.
	otherActivitySlackSeconds = 5
)

type vciSleepModeConfig struct {
	Status vciSleepModeStatus `json:"status"`
}

type vciSleepModeStatus struct {
	LastActivity     int64                `json:"lastActivity,omitempty"`
	LastActivityInfo *vciLastActivityInfo `json:"lastActivityInfo,omitempty"`
}

type vciLastActivityInfo struct {
	Subject string `json:"subject,omitempty"`
}

// sleepAfterSyncState tracks one VCI the watcher woke in wakeModeSync.
type sleepAfterSyncState struct {
	wokeAt time.Time
	// readyAt and readyActivity are captured on the first Ready pass after the
	// wake, once the watcher's own wake request has finished.
	readyAt       time.Time
	readyActivity int64
	// blocker is the last reason the VCI was not put back to sleep yet,
	// reported if the timeout is reached.
	blocker string
}

// parseWakeDefault reads WATCH_WAKE_DEFAULT. Empty keeps the original
// behavior: every VCI is woken unless its wake annotation says otherwise.
func parseWakeDefault(raw string) (wakeMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "enabled", "true":
		return wakeModeOn, nil
	case "disabled", "false":
		return wakeModeOff, nil
	case "sync":
		return wakeModeSync, nil
	default:
		return "", fmt.Errorf("WATCH_WAKE_DEFAULT must be enabled, disabled, or sync, got %q", raw)
	}
}

func parsePositiveDuration(name string, def time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", name)
	}
	return parsed, nil
}

func (cfg *watcherConfig) defaultWakeMode() wakeMode {
	if cfg.wakeDefault == "" {
		return wakeModeOn
	}
	return cfg.wakeDefault
}

func (cfg *watcherConfig) sleepAfterSyncTimeoutOrDefault() time.Duration {
	if cfg.sleepAfterSyncTimeout > 0 {
		return cfg.sleepAfterSyncTimeout
	}
	return defaultSleepAfterSyncTimeout
}

func (cfg *watcherConfig) sleepAfterSyncSettleOrDefault() time.Duration {
	if cfg.sleepAfterSyncSettle > 0 {
		return cfg.sleepAfterSyncSettle
	}
	return defaultSleepAfterSyncSettle
}

// wakeModeForVCI applies the VCI's wake annotation over WATCH_WAKE_DEFAULT.
// Pausing is unaffected by the mode: a sleeping VCI is always paused.
func wakeModeForVCI(cfg *watcherConfig, runtime *watcherRuntime, vci virtualClusterInstance) wakeMode {
	raw, ok := vci.Metadata.Annotations[cfg.wakeAnnotation()]
	if !ok {
		return cfg.defaultWakeMode()
	}
	switch mode := wakeMode(strings.ToLower(strings.TrimSpace(raw))); mode {
	case wakeModeOn, wakeModeOff, wakeModeSync:
		return mode
	default:
		logKey := vci.Metadata.Namespace + "/" + vci.Metadata.Name + "=" + raw
		if runtime != nil && !runtime.invalidWakeLogged[logKey] {
			runtime.invalidWakeLogged[logKey] = true
			log.Printf("ignoring invalid %s=%q on VCI %s/%s; want \"true\", \"false\", or \"sync\"", cfg.wakeAnnotation(), raw, vci.Metadata.Namespace, vci.Metadata.Name)
		}
		return cfg.defaultWakeMode()
	}
}

func describeWakeMode(cfg watcherConfig) string {
	key := cfg.wakeAnnotation()
	switch cfg.defaultWakeMode() {
	case wakeModeOff:
		return "waking only VCIs with " + key + "=true or sync"
	case wakeModeSync:
		return fmt.Sprintf("waking every VCI unless %s=false, and putting it back to sleep after the deploy (timeout %s)", key, cfg.sleepAfterSyncTimeoutOrDefault())
	default:
		return "waking every VCI unless " + key + "=false"
	}
}

// trackSleepAfterSync starts tracking a VCI the watcher just woke in sync
// mode. A retried wake keeps the original wake time, so the timeout counts
// from the first attempt.
func trackSleepAfterSync(runtime *watcherRuntime, key string, mode wakeMode, now time.Time) {
	if mode != wakeModeSync {
		delete(runtime.sleepAfterSync, key)
		return
	}
	if runtime.sleepAfterSync[key] == nil {
		runtime.sleepAfterSync[key] = &sleepAfterSyncState{wokeAt: now}
	}
}

// sleepAfterSyncIfDone runs on every Ready pass. For a VCI the watcher woke in
// sync mode, it puts the VCI back to sleep once the deploy has finished and
// nobody else has used the tenant cluster since the wake. It gives up, leaving
// the normal sleep timer in charge, when someone else was active or the
// timeout passes.
func sleepAfterSyncIfDone(ctx context.Context, cfg *watcherConfig, runtime *watcherRuntime, vci virtualClusterInstance, key string, apps []application, hasActiveWork bool) error {
	state := runtime.sleepAfterSync[key]
	if state == nil {
		return nil
	}
	name := vci.Metadata.Namespace + "/" + vci.Metadata.Name
	if wakeModeForVCI(cfg, runtime, vci) != wakeModeSync {
		delete(runtime.sleepAfterSync, key)
		return nil
	}

	now := time.Now()
	if now.Sub(state.wokeAt) > cfg.sleepAfterSyncTimeoutOrDefault() {
		delete(runtime.sleepAfterSync, key)
		log.Printf("leaving VCI %s awake: the deploy did not finish within %s of the wake (%s); the normal sleep timer applies", name, cfg.sleepAfterSyncTimeoutOrDefault(), state.blocker)
		return nil
	}

	activity, subject := vciLastActivity(vci)
	if state.readyAt.IsZero() {
		// The watcher's own wake request may have kept updating the activity
		// until the VCI became ready, so later comparisons start from here.
		state.readyAt = now
		state.readyActivity = activity
		state.blocker = "waiting for the deploy to start"
		return nil
	}
	if otherActivitySinceWake(cfg, state, activity, subject) {
		delete(runtime.sleepAfterSync, key)
		who := subject
		if who == "" {
			who = "another client"
		}
		log.Printf("leaving VCI %s awake after the deploy: %s used it after the watcher woke it; the normal sleep timer applies", name, who)
		return nil
	}

	if blocker := deployBlocker(cfg, now, state, apps, hasActiveWork); blocker != "" {
		state.blocker = blocker
		keepAwakeDuringDeploy(ctx, cfg, vci, activity, now)
		return nil
	}
	if blocker, err := kargoStagesBlocker(ctx, cfg, apps); err != nil || blocker != "" {
		if err != nil {
			blocker = err.Error()
		}
		state.blocker = blocker
		return nil
	}

	if err := cfg.api.patchVirtualClusterInstanceForceSleep(ctx, vci.Metadata.Namespace, vci.Metadata.Name); err != nil {
		state.blocker = "force sleep failed: " + err.Error()
		return fmt.Errorf("put VCI %s back to sleep after the deploy: %w", name, err)
	}
	delete(runtime.sleepAfterSync, key)
	log.Printf("put VCI %s back to sleep: the deploy finished %s after the wake and nobody else used it", name, now.Sub(state.wokeAt).Round(time.Second))
	return nil
}

// vciLastActivity returns the VCI's last sleep-mode activity time (Unix
// seconds) and its subject.
func vciLastActivity(vci virtualClusterInstance) (int64, string) {
	var activity int64
	subject := ""
	// The annotations are Platform's record. status.sleepModeConfig is filled in
	// only for ?extended=true reads, which the watcher does not make, so it is
	// just a fallback for API shapes that do carry it.
	if raw := strings.TrimSpace(vci.Metadata.Annotations[sleepModeLastActivityInfoAnno]); raw != "" {
		var info struct {
			Subject string `json:"subject"`
		}
		if err := json.Unmarshal([]byte(raw), &info); err == nil {
			subject = strings.TrimSpace(info.Subject)
		}
	}
	if config := vci.Status.SleepModeConfig; config != nil {
		activity = config.Status.LastActivity
		if subject == "" && config.Status.LastActivityInfo != nil {
			subject = strings.TrimSpace(config.Status.LastActivityInfo.Subject)
		}
	}
	if activity == 0 {
		if parsed, err := strconv.ParseInt(strings.TrimSpace(vci.Metadata.Annotations[sleepModeLastActivityAnno]), 10, 64); err == nil {
			activity = parsed
		}
	}
	return activity, subject
}

// otherActivitySinceWake reports whether anyone other than the watcher used
// the tenant cluster after the wake. With a known wake subject the subject
// decides. Without one, any activity newer than the first Ready pass counts.
func otherActivitySinceWake(cfg *watcherConfig, state *sleepAfterSyncState, activity int64, subject string) bool {
	if cfg.wakeSubject != "" && subject != "" {
		return subject != cfg.wakeSubject && activity >= state.wokeAt.Unix()
	}
	return activity > state.readyActivity+otherActivitySlackSeconds
}

// deployBlocker explains why the deploy does not count as finished yet, or
// returns "" when every matching Application is synced and healthy.
func deployBlocker(cfg *watcherConfig, now time.Time, state *sleepAfterSyncState, apps []application, hasActiveWork bool) string {
	if settle := cfg.sleepAfterSyncSettleOrDefault(); now.Sub(state.readyAt) < settle {
		return fmt.Sprintf("settling for %s after the VCI became ready", settle)
	}
	if len(apps) == 0 {
		return "no Argo CD Applications target this VCI"
	}
	if hasActiveWork {
		return "a sync, OutOfSync revision, or Kargo promotion is still in progress"
	}
	for _, app := range apps {
		switch {
		case applicationRefreshRequestFingerprint(app) != "":
			return "Application " + app.Metadata.Name + " has a pending refresh"
		case app.Operation != nil:
			return "Application " + app.Metadata.Name + " has a pending operation"
		case app.Status.OperationState != nil && strings.EqualFold(app.Status.OperationState.Phase, "Running"):
			return "Application " + app.Metadata.Name + " is syncing"
		case !strings.EqualFold(app.Status.Sync.Status, "Synced"):
			return "Application " + app.Metadata.Name + " is " + app.Status.Sync.Status
		case !strings.EqualFold(app.Status.Health.Status, "Healthy"):
			return "Application " + app.Metadata.Name + " health is " + app.Status.Health.Status
		}
	}
	return ""
}

type kargoStage struct {
	Status struct {
		Conditions []condition `json:"conditions"`
	} `json:"status"`
}

// kargoStagesBlocker requires every Kargo Stage behind the matching
// Applications to be Ready, which Kargo only reports once verification has
// passed. Sleeping during verification would fail it.
func kargoStagesBlocker(ctx context.Context, cfg *watcherConfig, apps []application) (string, error) {
	seen := map[string]bool{}
	var keys []string
	for _, app := range apps {
		if key := applicationAuthorizedStageKey(app); key != "" && !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		namespace, name, _ := strings.Cut(key, "/")
		var stage kargoStage
		path := "/apis/kargo.akuity.io/v1alpha1/namespaces/" + url.PathEscape(namespace) + "/stages/" + url.PathEscape(name)
		if err := cfg.api.getJSON(ctx, path, nil, &stage); err != nil {
			if isAPIStatus(err, http.StatusForbidden) || isAPIStatus(err, http.StatusNotFound) {
				return "", fmt.Errorf("cannot read Kargo Stage %s to confirm verification finished: %w", key, err)
			}
			return "", fmt.Errorf("get Kargo Stage %s: %w", key, err)
		}
		ready, ok := findCondition(stage.Status.Conditions, readyConditionType)
		if !ok || !strings.EqualFold(ready.Status, "True") {
			return "Kargo Stage " + key + " is not Ready yet", nil
		}
	}
	return "", nil
}

func (a *kubernetesAPI) patchVirtualClusterInstanceForceSleep(ctx context.Context, namespace, name string) error {
	return a.mergePatch(ctx, "/apis/management.loft.sh/v1/namespaces/"+url.PathEscape(namespace)+"/virtualclusterinstances/"+url.PathEscape(name), map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{sleepModeForceAnnotation: "true"},
		},
	})
}

// deployKeepAliveInterval is how stale the VCI's last activity may get while the
// watcher waits for a deploy, before it records activity of its own.
const deployKeepAliveInterval = time.Minute

// keepAwakeDuringDeploy records the watcher's own activity while a deploy it
// woke the VCI for is still running. Argo CD's traffic does not count as
// activity (Platform marks the integration's access key ignore-activity), so a
// deploy longer than the inactivity timeout would otherwise be put to sleep
// halfway. It needs the watcher's subject: without one, sync mode tells other
// users from the watcher by timestamps alone and would read this as someone
// else using the VCI.
func keepAwakeDuringDeploy(ctx context.Context, cfg *watcherConfig, vci virtualClusterInstance, activity int64, now time.Time) {
	if cfg.wakeSubject == "" || cfg.api == nil || now.Unix()-activity < int64(deployKeepAliveInterval/time.Second) {
		return
	}
	if err := cfg.api.patchVirtualClusterInstanceActivity(ctx, vci.Metadata.Namespace, vci.Metadata.Name, now.Unix(), cfg.wakeSubject); err != nil {
		log.Printf("keep-alive for VCI %s/%s during its deploy failed: %v", vci.Metadata.Namespace, vci.Metadata.Name, err)
	}
}
