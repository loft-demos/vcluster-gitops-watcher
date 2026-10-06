package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testWakeSubject = "loft:user:gitops-watcher"

// resleepAPI fakes the Kubernetes API for sleep-after-sync: it records force
// sleep patches on VCIs and serves Kargo Stages by "namespace/name".
type resleepAPI struct {
	forcePatches []string
	stages       map[string]string // "namespace/name" -> Ready condition status
	stageStatus  int
}

func (f *resleepAPI) server(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/virtualclusterinstances/"):
			body, _ := io.ReadAll(r.Body)
			var patch map[string]map[string]map[string]string
			_ = json.Unmarshal(body, &patch)
			if patch["metadata"]["annotations"][sleepModeForceAnnotation] != "true" {
				t.Errorf("unexpected VCI patch %s", body)
			}
			f.forcePatches = append(f.forcePatches, r.URL.Path)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/stages/"):
			if f.stageStatus != 0 {
				w.WriteHeader(f.stageStatus)
				_, _ = w.Write([]byte(`{}`))
				return
			}
			parts := strings.Split(r.URL.Path, "/")
			key := parts[len(parts)-3] + "/" + parts[len(parts)-1]
			ready, ok := f.stages[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"status":{"conditions":[{"type":"Ready","status":"` + ready + `"}]}}`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func resleepConfig(t *testing.T, fake *resleepAPI, wakeSubject string) *watcherConfig {
	server := fake.server(t)
	return &watcherConfig{
		api:                  &kubernetesAPI{client: server.Client(), apiBase: server.URL, bearerToken: "token"},
		wakeDefault:          wakeModeSync,
		wakeSubject:          wakeSubject,
		sleepAfterSyncSettle: time.Minute,
	}
}

func resleepVCI(lastActivity int64, subject string) virtualClusterInstance {
	return virtualClusterInstance{
		Metadata: metadata{Name: "team-a", Namespace: "p-demo"},
		Status: virtualClusterStatus{SleepModeConfig: &vciSleepModeConfig{Status: vciSleepModeStatus{
			LastActivity:     lastActivity,
			LastActivityInfo: &vciLastActivityInfo{Subject: subject},
		}}},
	}
}

func deployedApp(name string) application {
	return application{
		Metadata: metadata{Name: name},
		Status: applicationStatus{
			Sync:   applicationSync{Status: "Synced"},
			Health: healthStatus{Status: "Healthy"},
		},
	}
}

// readyForResleep returns runtime state as it is after the first Ready pass,
// with the settle period already over.
func readyForResleep(wokeAt time.Time, readyActivity int64) *watcherRuntime {
	runtime := newWatcherRuntime()
	runtime.sleepAfterSync["team-a"] = &sleepAfterSyncState{
		wokeAt:        wokeAt,
		readyAt:       time.Now().Add(-2 * time.Minute),
		readyActivity: readyActivity,
	}
	return runtime
}

func TestSleepAfterSyncPutsVCIBackToSleepWhenDeployFinished(t *testing.T) {
	fake := &resleepAPI{}
	cfg := resleepConfig(t, fake, testWakeSubject)
	wokeAt := time.Now().Add(-5 * time.Minute)
	runtime := newWatcherRuntime()
	trackSleepAfterSync(runtime, "team-a", wakeModeSync, wokeAt)
	vci := resleepVCI(wokeAt.Unix()+40, testWakeSubject)
	apps := []application{deployedApp("guestbook")}

	// First Ready pass only records the baseline.
	if err := sleepAfterSyncIfDone(context.Background(), cfg, runtime, vci, "team-a", apps, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.forcePatches) != 0 {
		t.Fatalf("expected no sleep on the first Ready pass")
	}
	runtime.sleepAfterSync["team-a"].readyAt = time.Now().Add(-2 * time.Minute)

	if err := sleepAfterSyncIfDone(context.Background(), cfg, runtime, vci, "team-a", apps, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.forcePatches) != 1 || !strings.HasSuffix(fake.forcePatches[0], "/namespaces/p-demo/virtualclusterinstances/team-a") {
		t.Fatalf("expected one force sleep patch on the VCI, got %v", fake.forcePatches)
	}
	if runtime.sleepAfterSync["team-a"] != nil {
		t.Fatalf("expected tracking to end after sleeping the VCI")
	}
}

func TestSleepAfterSyncWaitsForDeploy(t *testing.T) {
	notSynced := deployedApp("guestbook")
	notSynced.Status.Sync.Status = "OutOfSync"
	degraded := deployedApp("guestbook")
	degraded.Status.Health.Status = "Progressing"
	running := deployedApp("guestbook")
	running.Status.OperationState = &applicationOperationState{Phase: "Running"}
	refreshing := deployedApp("guestbook")
	refreshing.Metadata.Annotations = map[string]string{argocdClusterRefreshAnnotation: "hard"}

	tests := map[string]struct {
		apps          []application
		hasActiveWork bool
		settling      bool
	}{
		"out of sync":        {apps: []application{notSynced}},
		"not healthy":        {apps: []application{degraded}},
		"sync running":       {apps: []application{running}},
		"refresh pending":    {apps: []application{refreshing}},
		"active GitOps work": {apps: []application{deployedApp("guestbook")}, hasActiveWork: true},
		"no Applications":    {},
		"still settling":     {apps: []application{deployedApp("guestbook")}, settling: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fake := &resleepAPI{}
			cfg := resleepConfig(t, fake, testWakeSubject)
			wokeAt := time.Now().Add(-5 * time.Minute)
			runtime := readyForResleep(wokeAt, wokeAt.Unix())
			if test.settling {
				runtime.sleepAfterSync["team-a"].readyAt = time.Now()
			}

			if err := sleepAfterSyncIfDone(context.Background(), cfg, runtime, resleepVCI(wokeAt.Unix(), testWakeSubject), "team-a", test.apps, test.hasActiveWork); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(fake.forcePatches) != 0 {
				t.Fatalf("expected no sleep while the deploy is unfinished")
			}
			state := runtime.sleepAfterSync["team-a"]
			if state == nil || state.blocker == "" {
				t.Fatalf("expected tracking to continue with a recorded blocker, got %+v", state)
			}
		})
	}
}

func TestSleepAfterSyncLeavesVCIAwakeAfterOtherActivity(t *testing.T) {
	wokeAt := time.Now().Add(-5 * time.Minute)
	tests := map[string]struct {
		wakeSubject string
		activity    int64
		subject     string
	}{
		"another user, known wake subject":     {wakeSubject: testWakeSubject, activity: wokeAt.Unix() + 120, subject: "loft:user:alice"},
		"newer activity, unknown wake subject": {activity: wokeAt.Unix() + 120, subject: "loft:user:admin"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fake := &resleepAPI{}
			cfg := resleepConfig(t, fake, test.wakeSubject)
			runtime := readyForResleep(wokeAt, wokeAt.Unix()+30)

			if err := sleepAfterSyncIfDone(context.Background(), cfg, runtime, resleepVCI(test.activity, test.subject), "team-a", []application{deployedApp("guestbook")}, false); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(fake.forcePatches) != 0 {
				t.Fatalf("expected the VCI to stay awake after someone else used it")
			}
			if runtime.sleepAfterSync["team-a"] != nil {
				t.Fatalf("expected tracking to end so the normal timer applies")
			}
		})
	}
}

func TestSleepAfterSyncIgnoresOwnActivityWithUnknownSubject(t *testing.T) {
	fake := &resleepAPI{}
	cfg := resleepConfig(t, fake, "")
	wokeAt := time.Now().Add(-5 * time.Minute)
	runtime := readyForResleep(wokeAt, wokeAt.Unix()+30)

	// Activity unchanged since the first Ready pass: only the watcher's wake.
	if err := sleepAfterSyncIfDone(context.Background(), cfg, runtime, resleepVCI(wokeAt.Unix()+30, "loft:user:admin"), "team-a", []application{deployedApp("guestbook")}, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.forcePatches) != 1 {
		t.Fatalf("expected the VCI to be put back to sleep, got %d patches", len(fake.forcePatches))
	}
}

func TestSleepAfterSyncGivesUpAfterTimeout(t *testing.T) {
	fake := &resleepAPI{}
	cfg := resleepConfig(t, fake, testWakeSubject)
	cfg.sleepAfterSyncTimeout = 10 * time.Minute
	wokeAt := time.Now().Add(-11 * time.Minute)
	runtime := readyForResleep(wokeAt, wokeAt.Unix())

	if err := sleepAfterSyncIfDone(context.Background(), cfg, runtime, resleepVCI(wokeAt.Unix(), testWakeSubject), "team-a", []application{deployedApp("guestbook")}, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.forcePatches) != 0 || runtime.sleepAfterSync["team-a"] != nil {
		t.Fatalf("expected tracking to end without sleeping after the timeout")
	}
}

func TestSleepAfterSyncWaitsForKargoStageReady(t *testing.T) {
	app := deployedApp("guestbook")
	app.Metadata.Annotations = map[string]string{kargoAuthorizedStageAnnotation: "demo:prod"}
	wokeAt := time.Now().Add(-5 * time.Minute)

	for _, test := range []struct {
		name       string
		fake       *resleepAPI
		wantSleeps int
	}{
		{name: "verifying", fake: &resleepAPI{stages: map[string]string{"demo/prod": "False"}}},
		{name: "unreadable", fake: &resleepAPI{stageStatus: http.StatusForbidden}},
		{name: "ready", fake: &resleepAPI{stages: map[string]string{"demo/prod": "True"}}, wantSleeps: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := resleepConfig(t, test.fake, testWakeSubject)
			runtime := readyForResleep(wokeAt, wokeAt.Unix())
			if err := sleepAfterSyncIfDone(context.Background(), cfg, runtime, resleepVCI(wokeAt.Unix(), testWakeSubject), "team-a", []application{app}, false); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(test.fake.forcePatches) != test.wantSleeps {
				t.Fatalf("got %d force sleeps, want %d", len(test.fake.forcePatches), test.wantSleeps)
			}
		})
	}
}

func TestSleepAfterSyncStopsWhenModeChanges(t *testing.T) {
	fake := &resleepAPI{}
	cfg := resleepConfig(t, fake, testWakeSubject)
	wokeAt := time.Now().Add(-5 * time.Minute)
	runtime := readyForResleep(wokeAt, wokeAt.Unix())
	vci := resleepVCI(wokeAt.Unix(), testWakeSubject)
	vci.Metadata.Annotations = map[string]string{cfg.wakeAnnotation(): "true"}

	if err := sleepAfterSyncIfDone(context.Background(), cfg, runtime, vci, "team-a", []application{deployedApp("guestbook")}, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.forcePatches) != 0 || runtime.sleepAfterSync["team-a"] != nil {
		t.Fatalf("expected tracking to stop once the VCI is no longer in sync mode")
	}
}

func TestTrackSleepAfterSyncKeepsFirstWakeTime(t *testing.T) {
	runtime := newWatcherRuntime()
	first := time.Now().Add(-time.Minute)
	trackSleepAfterSync(runtime, "team-a", wakeModeSync, first)
	trackSleepAfterSync(runtime, "team-a", wakeModeSync, time.Now())
	if got := runtime.sleepAfterSync["team-a"].wokeAt; !got.Equal(first) {
		t.Fatalf("expected a retried wake to keep the first wake time")
	}
	trackSleepAfterSync(runtime, "team-a", wakeModeOn, time.Now())
	if runtime.sleepAfterSync["team-a"] != nil {
		t.Fatalf("expected a plain wake to stop tracking")
	}
}

func TestReconcileVCITracksSyncModeWake(t *testing.T) {
	for _, test := range []struct {
		mode      wakeMode
		wantTrack bool
	}{{mode: wakeModeSync, wantTrack: true}, {mode: wakeModeOn}} {
		const secretName = "loft-demo-vcluster-team-a"
		apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
		wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) }))
		cfg := watcherConfig{
			api:                          &kubernetesAPI{client: apiServer.Client(), apiBase: apiServer.URL, bearerToken: "token"},
			wakeRequester:                &wakeRequester{client: wakeServer.Client(), baseURL: wakeServer.URL, acceptedStatuses: parseStatusSet("502,504")},
			wakeRetryInterval:            time.Hour,
			wakeDefault:                  test.mode,
			argocdClusterSecretNamespace: "argocd",
			clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
			projectNamespacePrefixes:     []string{"p-"},
		}
		vci := virtualClusterInstance{Metadata: metadata{Name: "team-a", Namespace: "p-demo", Annotations: map[string]string{sleepingSinceAnnotation: "1711800000"}}}
		apps := map[string][]application{secretName: {{
			Metadata:  metadata{Name: "guestbook"},
			Operation: &applicationOperation{Sync: json.RawMessage(`{"revision":"abc123"}`)},
		}}}
		runtime := newWatcherRuntime()
		if err := reconcileVCI(context.Background(), &cfg, runtime, vci, reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", true)}, nil)); err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if got := runtime.sleepAfterSync[secretName] != nil; got != test.wantTrack {
			t.Fatalf("mode %q: tracking=%v, want %v", test.mode, got, test.wantTrack)
		}
		apiServer.Close()
		wakeServer.Close()
	}
}

func TestWakeSubjectFromWakeIdentity(t *testing.T) {
	if got := (wakeIdentity{user: "gitops-watcher"}).activitySubject(); got != "loft:user:gitops-watcher" {
		t.Fatalf("unexpected user subject %q", got)
	}
	if got := (wakeIdentity{team: "platform"}).activitySubject(); got != "loft:team:platform" {
		t.Fatalf("unexpected team subject %q", got)
	}
}
