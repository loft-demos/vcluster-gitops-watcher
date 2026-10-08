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

// platformAPI fakes the Kubernetes API for the Platform-integration tests: it
// records cluster Secret pause patches (true = pause) and Application health
// patches, and serves management.loft.sh ArgoCDApplications.
type platformAPI struct {
	pauses        []bool
	health        map[string]healthStatus
	argoCDApps    string // JSON items for the ArgoCDApplication list
	argoCDAppCode int
	argoCDLists   int
}

func (f *platformAPI) server(t *testing.T, secretName string) *httptest.Server {
	t.Helper()
	f.health = map[string]healthStatus{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			_, _ = w.Write([]byte(`{"metadata":{"annotations":{}}}`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			body, _ := io.ReadAll(r.Body)
			var patch struct {
				Metadata struct {
					Annotations map[string]*string `json:"annotations"`
				} `json:"metadata"`
			}
			_ = json.Unmarshal(body, &patch)
			v := patch.Metadata.Annotations[argocdSkipReconcileAnnotation]
			f.pauses = append(f.pauses, v != nil && *v == "true")
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/applications/"):
			body, _ := io.ReadAll(r.Body)
			var patch struct {
				Status struct {
					Health healthStatus `json:"health"`
				} `json:"status"`
			}
			_ = json.Unmarshal(body, &patch)
			parts := strings.Split(strings.TrimSuffix(r.URL.Path, "/status"), "/")
			f.health[parts[len(parts)-1]] = patch.Status.Health
		case r.Method == http.MethodGet && r.URL.Path == platformArgoCDApplicationsPath:
			f.argoCDLists++
			if f.argoCDAppCode != 0 {
				w.WriteHeader(f.argoCDAppCode)
				_, _ = w.Write([]byte(`{}`))
				return
			}
			_, _ = w.Write([]byte(`{"items":[` + f.argoCDApps + `]}`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func platformManagedApp(name string, status applicationStatus) application {
	return application{
		Metadata: metadata{Name: name, ResourceVersion: "5", Labels: map[string]string{loftManagedByLabel: platformArgoCDManagedByValue}},
		Status:   status,
	}
}

func healthyStatus() applicationStatus {
	return applicationStatus{
		ReconciledAt: "2026-10-08T15:00:00Z",
		Sync:         applicationSync{Status: "Synced", Revision: "abc"},
		Health:       healthStatus{Status: "Healthy"},
	}
}

func healthTestConfig(apiServer *httptest.Server) watcherConfig {
	cfg := newAppTestConfig(apiServer)
	cfg.patchApplicationHealth = true
	cfg.sleepingHealthMessage = "vCluster sleeping"
	cfg.wakingHealthMessage = "vCluster waking"
	return cfg
}

// While a tenant cluster sleeps, a Platform-managed Application keeps Healthy
// and only gets the sleeping message: Platform's Stack tasks count anything but
// Healthy+Synced as not ready and fail after their timeout.
func TestSleepingClusterKeepsPlatformApplicationsHealthy(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	fake := &platformAPI{}
	cfg := healthTestConfig(fake.server(t, secretName))
	runtime := newWatcherRuntime()

	platformApp := platformManagedApp("stack-backend", healthyStatus())
	userApp := application{Metadata: metadata{Name: "guestbook", ResourceVersion: "5"}, Status: healthyStatus()}
	apps := map[string][]application{secretName: {platformApp, userApp}}

	// Awake first, so the watcher sees the real health before the sleep.
	ready := reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", false)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, runtime, readyVCIForTest(), ready); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	asleep := reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", true)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, runtime, sleepingVCIForTest(), asleep); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	if got := fake.health["stack-backend"]; got.Status != "Healthy" || got.Message != "vCluster sleeping" {
		t.Errorf("Platform-managed app: got health %+v, want Healthy with the sleeping message", got)
	}
	if got := fake.health["guestbook"]; got.Status != "Suspended" {
		t.Errorf("user app: got health %+v, want Suspended as before", got)
	}
}

// An older watcher left Suspended on a Platform app (the stacks-demo case). With
// nothing remembered, the watcher replaces its own write with Healthy.
func TestSleepingClusterRepairsSuspendedLeftOnPlatformApplication(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	fake := &platformAPI{}
	cfg := healthTestConfig(fake.server(t, secretName))

	left := platformManagedApp("stack-backend", applicationStatus{
		ReconciledAt: "2026-10-08T15:00:00Z",
		Sync:         applicationSync{Status: "Synced", Revision: "abc"},
		Health:       healthStatus{Status: "Suspended", Message: "vCluster sleeping"},
	})
	idx := reconcileIndexForTest(map[string][]application{secretName: {left}}, []secret{clusterSecretForTest(secretName, "", true)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, newWatcherRuntime(), sleepingVCIForTest(), idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if got := fake.health["stack-backend"]; got.Status != "Healthy" {
		t.Fatalf("got health %+v, want the leftover Suspended replaced with Healthy", got)
	}
}

// Awake, Argo CD owns the health: a remembered Healthy must not overwrite a real
// Progressing on a Kargo- or Platform-managed Application.
func TestReadyClusterDoesNotOverwriteRealProgressingOnPlatformApplication(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	fake := &platformAPI{}
	cfg := healthTestConfig(fake.server(t, secretName))
	runtime := newWatcherRuntime()
	runtime.lastKnownKargoHealth["stack-backend"] = healthStatus{Status: "Healthy"}

	rolling := platformManagedApp("stack-backend", applicationStatus{
		ReconciledAt: "2026-10-08T15:00:00Z",
		Sync:         applicationSync{Status: "Synced", Revision: "abc"},
		Health:       healthStatus{Status: "Progressing", Message: "Waiting for rollout"},
	})
	idx := reconcileIndexForTest(map[string][]application{secretName: {rolling}}, []secret{clusterSecretForTest(secretName, "", false)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, runtime, readyVCIForTest(), idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if got, patched := fake.health["stack-backend"]; patched {
		t.Fatalf("real Progressing health was overwritten with %+v", got)
	}
	if len(fake.pauses) != 0 {
		t.Fatalf("expected no pause while the app is rolling out, got %v", fake.pauses)
	}
}

func TestPlatformTriggersByVCI(t *testing.T) {
	items := []platformArgoCDApplication{
		{Metadata: metadata{Name: "refresh", Namespace: "p-demo", Annotations: map[string]string{platformArgoCDRefreshAnnotation: "hard"}},
			Spec: platformArgoCDApplicationSpec{Destination: platformArgoCDDestination{VirtualCluster: &platformArgoCDDestinationVirtualCluster{Name: "team-a"}}}},
		{Metadata: metadata{Name: "sync", Namespace: "p-demo", Annotations: map[string]string{platformArgoCDSyncAnnotation: `{"prune":true}`}},
			Spec: platformArgoCDApplicationSpec{Destination: platformArgoCDDestination{VirtualCluster: &platformArgoCDDestinationVirtualCluster{Name: "team-a"}}}},
		{Metadata: metadata{Name: "idle", Namespace: "p-demo"},
			Spec: platformArgoCDApplicationSpec{Destination: platformArgoCDDestination{VirtualCluster: &platformArgoCDDestinationVirtualCluster{Name: "team-a"}}}},
		{Metadata: metadata{Name: "host-cluster", Namespace: "p-demo", Annotations: map[string]string{platformArgoCDRefreshAnnotation: "hard"}}},
	}
	got := platformTriggersByVCI(items)["p-demo/team-a"]
	if len(got) != 2 || got["argocdapplication/refresh"] == "" || got["argocdapplication/sync"] == "" {
		t.Fatalf("expected the refresh and sync requests for p-demo/team-a, got %v", got)
	}
	if len(platformTriggersByVCI(items)) != 1 {
		t.Fatalf("an ArgoCDApplication without a virtual cluster destination must be ignored")
	}
}

// A Stack retry or a Platform UI Refresh on an awake cluster the watcher paused:
// Platform's refresh call needs Argo CD to reconcile within 30s, so the cluster
// is un-paused for as long as the request is pending.
func TestReadyClusterUnpausesForPendingPlatformRefresh(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	fake := &platformAPI{}
	cfg := newAppTestConfig(fake.server(t, secretName))
	runtime := newWatcherRuntime()

	idx := reconcileIndexForTest(map[string][]application{secretName: {platformManagedApp("stack-backend", healthyStatus())}},
		[]secret{clusterSecretForTest(secretName, "", true)}, nil)
	idx.platformTriggers = map[string]map[string]string{"p-demo/team-a": {"argocdapplication/stack-backend": "refresh=hard;sync="}}

	for i := 0; i < 3; i++ {
		if err := reconcileVCI(context.Background(), &cfg, runtime, readyVCIForTest(), idx); err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		idx.clusterSecrets = buildClusterSecretIndex([]secret{clusterSecretForTest(secretName, "", false)})
	}
	if len(fake.pauses) != 1 || fake.pauses[0] {
		t.Fatalf("expected one resume and no re-pause while the Platform request is pending, got %v", fake.pauses)
	}
}

func wakeCountingConfig(t *testing.T, fake *platformAPI, secretName string, wakeCalls *int) watcherConfig {
	t.Helper()
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(wakeServer.Close)
	cfg := newAppTestConfig(fake.server(t, secretName))
	cfg.wakeRequester = &wakeRequester{client: wakeServer.Client(), baseURL: wakeServer.URL, acceptedStatuses: parseStatusSet("504")}
	cfg.wakeRetryInterval = time.Hour
	return cfg
}

func TestSleepingClusterWakesForPlatformRequest(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	fake := &platformAPI{}
	wakes := 0
	cfg := wakeCountingConfig(t, fake, secretName, &wakes)
	runtime := newWatcherRuntime()

	idx := reconcileIndexForTest(map[string][]application{secretName: {platformManagedApp("stack-backend", healthyStatus())}},
		[]secret{clusterSecretForTest(secretName, "", true)}, nil)
	idx.platformTriggers = map[string]map[string]string{"p-demo/team-a": {"argocdapplication/stack-backend": "refresh=hard;sync={}"}}

	for i := 0; i < 2; i++ {
		if err := reconcileVCI(context.Background(), &cfg, runtime, sleepingVCIForTest(), idx); err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
	}
	if wakes != 1 {
		t.Fatalf("expected one wake for the Platform request (no repeat within the retry interval), got %d", wakes)
	}
}

// A Stack deployed to a sleeping cluster: Platform creates the Argo CD
// Application, Argo CD cannot reconcile it while paused, so the watcher wakes the
// cluster. A user's own new Application still does not.
func TestSleepingClusterWakesForNewPlatformApplicationOnly(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	newPlatformApp := platformManagedApp("stack-frontend", applicationStatus{})
	newUserApp := application{Metadata: metadata{Name: "guestbook", ResourceVersion: "1"}}

	for name, tc := range map[string]struct {
		app   application
		wakes int
	}{"Platform": {newPlatformApp, 1}, "user": {newUserApp, 0}} {
		fake := &platformAPI{}
		wakes := 0
		cfg := wakeCountingConfig(t, fake, secretName, &wakes)
		idx := reconcileIndexForTest(map[string][]application{secretName: {tc.app}}, []secret{clusterSecretForTest(secretName, "", true)}, nil)
		if err := reconcileVCI(context.Background(), &cfg, newWatcherRuntime(), sleepingVCIForTest(), idx); err != nil {
			t.Fatalf("%s: unexpected reconcile error: %v", name, err)
		}
		if wakes != tc.wakes {
			t.Errorf("%s: got %d wakes, want %d", name, wakes, tc.wakes)
		}
	}
}

func TestListPlatformArgoCDApplicationsOptionalBacksOffWhenNotReadable(t *testing.T) {
	fake := &platformAPI{argoCDAppCode: http.StatusForbidden}
	cfg := newAppTestConfig(fake.server(t, "unused"))
	runtime := newWatcherRuntime()

	for i := 0; i < 3; i++ {
		if items := listPlatformArgoCDApplicationsOptional(context.Background(), &cfg, runtime); items != nil {
			t.Fatalf("expected no items on 403, got %v", items)
		}
	}
	if fake.argoCDLists != 1 {
		t.Fatalf("expected one list attempt, then a back-off, got %d", fake.argoCDLists)
	}
}

// Platform answers 502 when it skips the wake (an ignored identity or user agent,
// forced-duration sleep); only 504 means the held request outlived a gateway.
func TestWakeRequestTreats502AsFailureAndSendsUserAgent(t *testing.T) {
	var userAgent string
	status := http.StatusBadGateway
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userAgent = r.UserAgent()
		w.WriteHeader(status)
	}))
	defer server.Close()
	w := &wakeRequester{client: server.Client(), baseURL: server.URL, acceptedStatuses: parseStatusSet("504")}

	if err := w.Execute(context.Background(), "demo", "p-demo", "team-a"); err == nil {
		t.Fatal("expected a 502 to be reported as a failed wake")
	}
	if userAgent != wakeUserAgent {
		t.Fatalf("User-Agent = %q, want %q", userAgent, wakeUserAgent)
	}
	status = http.StatusGatewayTimeout
	if err := w.Execute(context.Background(), "demo", "p-demo", "team-a"); err != nil {
		t.Fatalf("expected a 504 to count as a started wake, got %v", err)
	}
}

// Platform refuses wakes (400 ForcedSleeping) during scheduled and
// forced-duration sleep, so the watcher does not try.
func TestSleepingClusterNotWokenDuringScheduledOrForcedDurationSleep(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	drifted := application{Metadata: metadata{Name: "guestbook", ResourceVersion: "7"},
		Status: applicationStatus{ReconciledAt: "2026-10-08T15:00:00Z", Sync: applicationSync{Status: "OutOfSync", Revision: "abc"}, Health: healthStatus{Status: "Healthy"}}}

	for sleepType, want := range map[string]int{"scheduledSleep": 0, "forcedDurationSleep": 0, "inactivitySleep": 1} {
		fake := &platformAPI{}
		wakes := 0
		cfg := wakeCountingConfig(t, fake, secretName, &wakes)
		vci := sleepingVCIForTest()
		vci.Metadata.Annotations[sleepTypeAnnotation] = sleepType
		idx := reconcileIndexForTest(map[string][]application{secretName: {drifted}}, []secret{clusterSecretForTest(secretName, "", true)}, nil)
		if err := reconcileVCI(context.Background(), &cfg, newWatcherRuntime(), vci, idx); err != nil {
			t.Fatalf("%s: unexpected reconcile error: %v", sleepType, err)
		}
		if wakes != want {
			t.Errorf("%s: got %d wakes, want %d", sleepType, wakes, want)
		}
	}
}

func TestVCIUnmanagedReasonForClustersThatCannotSleep(t *testing.T) {
	cfg := &watcherConfig{}
	standaloneValues := readyVCIForTest()
	standaloneValues.Status.VirtualCluster = &vciTemplate{HelmRelease: vciHelmRelease{Values: "controlPlane:\n  standalone:\n    enabled: true\n"}}
	standaloneSpec := readyVCIForTest()
	standaloneSpec.Spec.Standalone = true
	external := readyVCIForTest()
	external.Spec.External = true
	externalConnected := readyVCIForTest()
	externalConnected.Spec.External = true
	externalConnected.Spec.ClusterRef = vciClusterRef{Cluster: "loft-cluster", Namespace: "loft-p-demo"}
	workloadsOnly := readyVCIForTest()
	workloadsOnly.Metadata.Annotations = map[string]string{sleepScopeAnnotation: "workloads-only"}

	for name, tc := range map[string]struct {
		vci       virtualClusterInstance
		unmanaged bool
	}{
		"standalone in vcluster.yaml":   {standaloneValues, true},
		"standalone in spec":            {standaloneSpec, true},
		"external, not connected":       {external, true},
		"external, connected":           {externalConnected, false},
		"workloads-only sleep scope":    {workloadsOnly, true},
		"ordinary shared-nodes cluster": {readyVCIForTest(), false},
	} {
		if got := vciUnmanagedReason(cfg, tc.vci) != ""; got != tc.unmanaged {
			t.Errorf("%s: unmanaged = %v, want %v", name, got, tc.unmanaged)
		}
	}
}

func TestYAMLBoolAtNestedPaths(t *testing.T) {
	doc := "controlPlane:\n  distro:\n    k8s:\n      enabled: true\n  standalone:\n    enabled: true # on\nprivateNodes: {enabled: false}\n"
	if !yamlBoolAt(doc, "controlPlane", "standalone", "enabled") {
		t.Error("controlPlane.standalone.enabled should be true")
	}
	if yamlBoolAt(doc, "controlPlane", "enabled") {
		t.Error("a nested enabled must not count for controlPlane.enabled")
	}
	if yamlBoolAt(doc, "privateNodes", "enabled") {
		t.Error("flow-style privateNodes.enabled is false")
	}
}

// Platform keeps the activity subject in the last-activity-info annotation; the
// watcher's list never carries status.sleepModeConfig.
func TestVCILastActivityReadsSubjectFromAnnotation(t *testing.T) {
	vci := virtualClusterInstance{Metadata: metadata{Annotations: map[string]string{
		sleepModeLastActivityAnno:     "1791494100",
		sleepModeLastActivityInfoAnno: `{"subject":"loft:user:alice","verb":"get"}`,
	}}}
	activity, subject := vciLastActivity(vci)
	if activity != 1791494100 || subject != "loft:user:alice" {
		t.Fatalf("got activity %d subject %q", activity, subject)
	}
}
