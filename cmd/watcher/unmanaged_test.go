package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPrivateNodesEnabled(t *testing.T) {
	cases := map[string]struct {
		values string
		want   bool
	}{
		"block enabled":                  {"privateNodes:\n  enabled: true\n  vpn:\n    enabled: true\n", true},
		"enabled after other child keys": {"controlPlane:\n  distro:\n    k8s: {}\nprivateNodes:\n  autoNodes:\n    - provider: metal3\n      static: []\n  enabled: true # comment\n", true},
		"flow mapping":                   {"privateNodes: {enabled: true}\n", true},
		"disabled":                       {"privateNodes:\n  enabled: false\n", false},
		"absent":                         {"sync:\n  toHost:\n    pods:\n      enabled: true\n", false},
		"only a nested enabled":          {"privateNodes:\n  vpn:\n    enabled: true\n", false},
		"enabled under another key":      {"sleep:\n  enabled: true\nprivateNodes:\n  vpn: {}\n", false},
		"commented out":                  {"# privateNodes:\n#   enabled: true\n", false},
		"quoted string is not a boolean": {"privateNodes:\n  enabled: \"true\"\n", false},
		"empty":                          {"", false},
	}
	for name, tc := range cases {
		if got := privateNodesEnabled(tc.values); got != tc.want {
			t.Errorf("%s: privateNodesEnabled = %v, want %v", name, got, tc.want)
		}
	}
}

func privateNodesVCI(fromTemplate bool) virtualClusterInstance {
	vci := readyVCIForTest()
	values := &vciTemplate{HelmRelease: vciHelmRelease{Values: "privateNodes:\n  enabled: true\n"}}
	if fromTemplate {
		vci.Status.VirtualCluster = values
	} else {
		vci.Spec.Template = values
	}
	return vci
}

func TestVCIUnmanagedReason(t *testing.T) {
	cfg := &watcherConfig{}
	if reason := vciUnmanagedReason(cfg, privateNodesVCI(true)); reason == "" {
		t.Error("templated private-nodes VCI (values only in status.virtualCluster) should be unmanaged")
	}
	if reason := vciUnmanagedReason(cfg, privateNodesVCI(false)); reason == "" {
		t.Error("private-nodes VCI with values in spec.template should be unmanaged")
	}
	if reason := vciUnmanagedReason(cfg, readyVCIForTest()); reason != "" {
		t.Errorf("shared-node VCI should be managed, got %q", reason)
	}

	optOut := readyVCIForTest()
	optOut.Metadata.Annotations = map[string]string{cfg.manageAnnotation(): "false"}
	if reason := vciUnmanagedReason(cfg, optOut); reason == "" {
		t.Error("manage=false should opt a VCI out")
	}

	forced := privateNodesVCI(true)
	forced.Metadata.Annotations = map[string]string{cfg.manageAnnotation(): "true"}
	if reason := vciUnmanagedReason(cfg, forced); reason != "" {
		t.Errorf("manage=true should keep a private-nodes VCI managed, got %q", reason)
	}
}

func TestReconcileVCIResumesPausedPrivateNodesClusterAndLeavesItAlone(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	var patches []bool
	apiServer := newAppTestAPI(t, secretName, &patches)
	defer apiServer.Close()
	cfg := newAppTestConfig(apiServer)
	cfg.patchApplicationHealth = true
	runtime := newWatcherRuntime()

	// Healthy and idle: a managed cluster would be paused.
	app := application{
		Metadata: metadata{Name: "gpu-operator", ResourceVersion: "3"},
		Status: applicationStatus{
			ReconciledAt: "2026-10-08T15:49:29Z",
			Sync:         applicationSync{Status: "Synced", Revision: "v26.3.3"},
			Health:       healthStatus{Status: "Healthy"},
		},
	}
	apps := map[string][]application{secretName: {app}}

	paused := reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", true)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, runtime, privateNodesVCI(true), paused); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if len(patches) != 1 || patches[0] {
		t.Fatalf("expected one resume patch for a paused private-nodes cluster, got %v", patches)
	}

	unpaused := reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", false)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, runtime, privateNodesVCI(true), unpaused); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if len(patches) != 1 {
		t.Fatalf("expected no further patches for an idle private-nodes cluster, got %v", patches)
	}
}

func TestReconcileVCIStillPausesIdleSharedNodeCluster(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	var patches []bool
	apiServer := newAppTestAPI(t, secretName, &patches)
	defer apiServer.Close()
	cfg := newAppTestConfig(apiServer)

	app := application{
		Metadata: metadata{Name: "guestbook", ResourceVersion: "3"},
		Status: applicationStatus{
			ReconciledAt: "2026-10-08T15:49:29Z",
			Sync:         applicationSync{Status: "Synced", Revision: "abc"},
			Health:       healthStatus{Status: "Healthy"},
		},
	}
	idx := reconcileIndexForTest(map[string][]application{secretName: {app}}, []secret{clusterSecretForTest(secretName, "", false)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, newWatcherRuntime(), readyVCIForTest(), idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if len(patches) != 1 || !patches[0] {
		t.Fatalf("expected the idle shared-node cluster to be paused as before, got %v", patches)
	}
}

// The lab case: nv-gpu-opera-test and nv-gpu-operator-test share their first 40
// name characters. While one has no Secret yet, it must not take the other's.
func TestResolveQueryDoesNotTakeAnotherVCIsSecretBySharedPrefix(t *testing.T) {
	templates := parseList(defaultClusterSecretNameTemplates)
	operaNames := expandClusterSecretNames(templates, "default", "nv-gpu-opera-test")
	operaTruncated := safeConcatNameMax(operaNames[1], clusterNameMaxLength) // the "-argocd" v2 name

	labeled := labeledClusterSecret("cluster-opera", operaTruncated, "https://vcp.example.com/kubernetes/project/default/virtualcluster/nv-gpu-opera-test", "p-default", "nv-gpu-opera-test")
	unlabeled := v2ClusterSecret("cluster-opera-unlabeled", operaTruncated, "")

	for name, s := range map[string]secret{"labeled": labeled, "unlabeled": unlabeled} {
		index := buildClusterSecretIndex([]secret{s})
		resolved := index.resolveQuery(clusterSecretQuery{
			expectedNames: expandClusterSecretNames(templates, "default", "nv-gpu-operator-test"),
			instanceKey:   "p-default/nv-gpu-operator-test",
		})
		if resolved.secret != nil {
			t.Errorf("%s: nv-gpu-operator-test resolved to %s, which belongs to nv-gpu-opera-test", name, resolved.secret.Metadata.Name)
		}
	}

	// Its own Secret still resolves, by the exact truncated name.
	index := buildClusterSecretIndex([]secret{unlabeled})
	resolved := index.resolveQuery(clusterSecretQuery{expectedNames: operaNames, instanceKey: "p-default/nv-gpu-opera-test"})
	if resolved.secret == nil || resolved.secret.Metadata.Name != "cluster-opera-unlabeled" {
		t.Fatalf("expected nv-gpu-opera-test to resolve its own truncated Secret, got %+v", resolved)
	}
}

func TestResolveQueryLongProjectDoesNotMatchAnyVCIByPrefix(t *testing.T) {
	templates := parseList(defaultClusterSecretNameTemplates)
	project := "ml-platform-research"
	other := safeConcatNameMax(expandClusterSecretNames(templates, project, "a")[1], clusterNameMaxLength)
	index := buildClusterSecretIndex([]secret{v2ClusterSecret("cluster-a", other, "")})

	resolved := index.resolveQuery(clusterSecretQuery{
		expectedNames: expandClusterSecretNames(templates, project, "totally-different"),
		instanceKey:   "p-" + project + "/totally-different",
	})
	if resolved.secret != nil {
		t.Fatalf("totally-different resolved to %s, VCI a's Secret", resolved.secret.Metadata.Name)
	}
}

func TestResolveQueryExactNameMatchSkipsSecretLabeledForAnotherVCI(t *testing.T) {
	templates := parseList(defaultClusterSecretNameTemplates)
	expected := expandClusterSecretNames(templates, "default", "team-a")
	index := buildClusterSecretIndex([]secret{labeledClusterSecret("cluster-b", expected[0], "", "p-default", "team-b")})

	resolved := index.resolveQuery(clusterSecretQuery{expectedNames: expected, instanceKey: "p-default/team-a"})
	if resolved.secret != nil {
		t.Fatalf("team-a resolved to %s, labeled for team-b", resolved.secret.Metadata.Name)
	}
}

func sleepingVCIForTest() virtualClusterInstance {
	return virtualClusterInstance{Metadata: metadata{
		Name:        "team-a",
		Namespace:   "p-demo",
		Annotations: map[string]string{sleepingSinceAnnotation: "1711800000"},
	}}
}

// An Application that stays OutOfSync after the watcher already woke the VCI for
// it must not wake the VCI again every time it falls back asleep.
func TestReconcileVCIDoesNotRewakeForAnAlreadyHandledOutOfSyncRevision(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	var patches []bool
	apiServer := newAppTestAPI(t, secretName, &patches)
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := newAppTestConfig(apiServer)
	cfg.wakeRequester = &wakeRequester{client: wakeServer.Client(), baseURL: wakeServer.URL, acceptedStatuses: parseStatusSet("502,504")}
	cfg.wakeRetryInterval = time.Millisecond
	runtime := newWatcherRuntime()

	drifted := application{
		Metadata: metadata{Name: "guestbook", ResourceVersion: "7"},
		Status: applicationStatus{
			ReconciledAt: "2026-10-08T15:00:00Z",
			Sync:         applicationSync{Status: "OutOfSync", Revision: "abc"},
			Health:       healthStatus{Status: "Healthy"},
		},
	}
	apps := map[string][]application{secretName: {drifted}}
	sleeping := reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", true)}, nil)
	ready := reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", false)}, nil)

	// The first sleep wakes it, as before.
	if err := reconcileVCI(context.Background(), &cfg, runtime, sleepingVCIForTest(), sleeping); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected the first OutOfSync revision to wake the VCI, got %d wakes", wakeCalls)
	}
	// It wakes, the deploy runs, and it falls back asleep with the app still OutOfSync.
	if err := reconcileVCI(context.Background(), &cfg, runtime, readyVCIForTest(), ready); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := reconcileVCI(context.Background(), &cfg, runtime, sleepingVCIForTest(), sleeping); err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
	}
	if wakeCalls != 1 {
		t.Fatalf("expected no re-wake for the same OutOfSync revision, got %d wakes", wakeCalls)
	}
}

// A wake that did not take is still retried after the interval.
func TestReconcileVCIStillRetriesAWakeThatDidNotTake(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	var patches []bool
	apiServer := newAppTestAPI(t, secretName, &patches)
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := newAppTestConfig(apiServer)
	cfg.wakeRequester = &wakeRequester{client: wakeServer.Client(), baseURL: wakeServer.URL, acceptedStatuses: parseStatusSet("502,504")}
	cfg.wakeRetryInterval = time.Millisecond
	runtime := newWatcherRuntime()

	app := application{
		Metadata: metadata{Name: "guestbook", ResourceVersion: "7"},
		Status:   applicationStatus{Sync: applicationSync{Status: "OutOfSync", Revision: "abc"}, Health: healthStatus{Status: "Healthy"}},
	}
	idx := reconcileIndexForTest(map[string][]application{secretName: {app}}, []secret{clusterSecretForTest(secretName, "", true)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, runtime, sleepingVCIForTest(), idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := reconcileVCI(context.Background(), &cfg, runtime, sleepingVCIForTest(), idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 2 {
		t.Fatalf("expected the wake to be retried while the VCI is still asleep, got %d wakes", wakeCalls)
	}
}

// A refresh requested on an awake cluster the watcher paused as idle must reach
// Argo CD: the cluster is un-paused and stays so within the refresh grace.
func TestReconcileVCIUnpausesReadyClusterForNewRefreshRequest(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	var patches []bool
	apiServer := newAppTestAPI(t, secretName, &patches)
	defer apiServer.Close()
	cfg := newAppTestConfig(apiServer)
	cfg.readyRefreshGrace = time.Minute
	runtime := newWatcherRuntime()

	app := application{
		Metadata: metadata{Name: "guestbook", ResourceVersion: "9", Annotations: map[string]string{argocdClusterRefreshAnnotation: "normal"}},
		Status: applicationStatus{
			ReconciledAt: "2026-10-08T15:00:00Z",
			Sync:         applicationSync{Status: "Synced", Revision: "abc"},
			Health:       healthStatus{Status: "Healthy"},
		},
	}
	apps := map[string][]application{secretName: {app}}

	paused := reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", true)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, runtime, readyVCIForTest(), paused); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if len(patches) != 1 || patches[0] {
		t.Fatalf("expected one resume patch for a new refresh request, got %v", patches)
	}

	unpaused := reconcileIndexForTest(apps, []secret{clusterSecretForTest(secretName, "", false)}, nil)
	if err := reconcileVCI(context.Background(), &cfg, runtime, readyVCIForTest(), unpaused); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if len(patches) != 1 {
		t.Fatalf("expected the cluster to stay un-paused while the refresh is pending in grace, got %v", patches)
	}
}
