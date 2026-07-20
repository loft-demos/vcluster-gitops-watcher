package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// clusterSecretForTest builds an Argo CD cluster Secret as it would appear in
// the index: a real metadata.name plus base64 data.name (and optional server).
// For the legacy v1 layout, metadata.name equals data.name.
func clusterSecretForTest(name, server string, paused bool) secret {
	annotations := map[string]string{}
	if paused {
		annotations[argocdSkipReconcileAnnotation] = "true"
	}
	data := map[string]string{"name": base64.StdEncoding.EncodeToString([]byte(name))}
	if server != "" {
		data["server"] = base64.StdEncoding.EncodeToString([]byte(server))
	}
	return secret{
		Metadata: metadata{Name: name, Annotations: annotations},
		Data:     data,
	}
}

// reconcileIndexForTest assembles a reconcileIndex from name-keyed Applications,
// cluster Secrets, and optional Kargo triggers, mirroring what reconcileAll
// builds once per pass.
func reconcileIndexForTest(appsByName map[string][]application, secrets []secret, kargo map[string]kargoWakeTrigger) reconcileIndex {
	if appsByName == nil {
		appsByName = map[string][]application{}
	}
	return reconcileIndex{
		appsByDestinationName:   appsByName,
		appsByDestinationServer: applicationsByDestinationServer(flattenApps(appsByName)),
		clusterSecrets:          buildClusterSecretIndex(secrets),
		kargoWakeTriggers:       kargo,
	}
}

func flattenApps(appsByName map[string][]application) []application {
	var all []application
	for _, apps := range appsByName {
		all = append(all, apps...)
	}
	return all
}

func TestProjectFromVCIUsesProjectLabelFirst(t *testing.T) {
	vci := virtualClusterInstance{
		Metadata: metadata{
			Namespace: "p-wrong",
			Labels: map[string]string{
				loftProjectLabel: "right-project",
			},
		},
	}

	if got := projectFromVCI(vci, []string{"p-", "loft-p-"}); got != "right-project" {
		t.Fatalf("expected project label to win, got %q", got)
	}
}

func TestProjectFromVCIDerivesProjectFromNamespacePrefixes(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		want      string
	}{
		{name: "short prefix", namespace: "p-default", want: "default"},
		{name: "loft prefix", namespace: "loft-p-demo", want: "demo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vci := virtualClusterInstance{Metadata: metadata{Namespace: tt.namespace}}
			if got := projectFromVCI(vci, []string{"p-", "loft-p-"}); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestClassifyVCISleepingWhenSleepAnnotationsArePresent(t *testing.T) {
	vci := virtualClusterInstance{
		Metadata: metadata{
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
		Status: virtualClusterStatus{
			Phase: "Ready",
			Conditions: []condition{
				{Type: virtualClusterOnlineConditionType, Status: "True"},
			},
		},
	}

	if got := classifyVCI(vci, false); got != vciStateSleeping {
		t.Fatalf("expected Sleeping, got %s", got)
	}
}

func TestClassifyVCIReadyWhenOnlineConditionIsTrue(t *testing.T) {
	vci := virtualClusterInstance{
		Status: virtualClusterStatus{
			Conditions: []condition{
				{Type: virtualClusterOnlineConditionType, Status: "True"},
			},
		},
	}

	if got := classifyVCI(vci, true); got != vciStateReady {
		t.Fatalf("expected Ready, got %s", got)
	}
}

func TestClassifyVCIReadyWhenStatusOnlineIsTrue(t *testing.T) {
	online := true
	vci := virtualClusterInstance{
		Status: virtualClusterStatus{
			Online: &online,
		},
	}

	if got := classifyVCI(vci, false); got != vciStateReady {
		t.Fatalf("expected Ready, got %s", got)
	}
}

func TestClassifyVCISleepingWhenOnlineConditionFalseAndSleepHintPresent(t *testing.T) {
	vci := virtualClusterInstance{
		Status: virtualClusterStatus{
			Phase:   "Sleeping",
			Message: "virtual cluster is sleeping",
			Conditions: []condition{
				{
					Type:    virtualClusterOnlineConditionType,
					Status:  "False",
					Message: "cluster sleeping",
				},
			},
		},
	}

	if got := classifyVCI(vci, false); got != vciStateSleeping {
		t.Fatalf("expected Sleeping, got %s", got)
	}
}

func TestClassifyVCIReadyForObservedAwakeShape(t *testing.T) {
	online := true
	vci := virtualClusterInstance{
		Metadata: metadata{
			Namespace: "p-api-framework",
			Annotations: map[string]string{
				"sleepmode.loft.sh/current-epoch-slept": "51523",
				"sleepmode.loft.sh/scheduled-wakeup":    "1774883460",
			},
		},
		Status: virtualClusterStatus{
			Phase:  "Ready",
			Online: &online,
			Conditions: []condition{
				{Type: readyConditionType, Status: "True"},
				{Type: virtualClusterOnlineConditionType, Status: "True"},
				{Type: virtualClusterReadyConditionType, Status: "True"},
			},
		},
	}

	if got := classifyVCI(vci, true); got != vciStateReady {
		t.Fatalf("expected Ready, got %s", got)
	}
}

func TestClassifyVCISleepingWhenVirtualClusterReadyConditionSaysSleeping(t *testing.T) {
	vci := virtualClusterInstance{
		Status: virtualClusterStatus{
			Conditions: []condition{
				{
					Type:    readyConditionType,
					Status:  "False",
					Reason:  "Sleeping",
					Message: "Virtual Cluster is sleeping",
				},
				{
					Type:    virtualClusterOnlineConditionType,
					Status:  "False",
					Reason:  "NetworkPeerOffline",
					Message: "vCluster seems to be offline",
				},
				{
					Type:    virtualClusterReadyConditionType,
					Status:  "False",
					Reason:  "Sleeping",
					Message: "Virtual Cluster is sleeping",
				},
			},
		},
	}

	if got := classifyVCI(vci, false); got != vciStateSleeping {
		t.Fatalf("expected Sleeping, got %s", got)
	}
}

func TestClassifyVCIWakingWhenPausedAndNotOtherwiseReadyOrSleeping(t *testing.T) {
	vci := virtualClusterInstance{
		Status: virtualClusterStatus{
			Phase: "Starting",
			Conditions: []condition{
				{Type: virtualClusterOnlineConditionType, Status: "False"},
			},
		},
	}

	if got := classifyVCI(vci, true); got != vciStateWaking {
		t.Fatalf("expected Waking, got %s", got)
	}
}

func TestApplicationsNeedReadyRefreshOnlyForManagedHealth(t *testing.T) {
	cfg := watcherConfig{
		patchApplicationHealth: true,
		sleepingHealthMessage:  "vCluster sleeping",
		wakingHealthMessage:    "vCluster waking",
	}

	apps := []application{
		{
			Status: applicationStatus{
				Health: healthStatus{
					Status:  "Suspended",
					Message: "vCluster sleeping",
				},
			},
		},
	}

	if !applicationsNeedReadyRefresh(apps, cfg) {
		t.Fatal("expected ready refresh for managed health")
	}

	apps[0].Status.Health.Status = "Healthy"
	if !applicationsNeedReadyRefresh(apps, cfg) {
		t.Fatal("expected ready refresh for stale managed health message")
	}

	apps[0].Status.Health.Message = "manual override"
	if applicationsNeedReadyRefresh(apps, cfg) {
		t.Fatal("did not expect ready refresh for unrelated app health")
	}
}

func TestPatchApplicationsHealthSkipsKargoManagedApps(t *testing.T) {
	var patched []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Fatalf("expected PATCH request, got %s", r.Method)
		}
		patched = append(patched, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      server.Client(),
			apiBase:     server.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace: "argocd",
		patchApplicationHealth:     true,
		applicationHealthPatchMode: applicationHealthPatchModeStatus,
	}

	apps := []application{
		{Metadata: metadata{Name: "plain-app"}},
		{
			Metadata: metadata{
				Name: "kargo-app",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "demo:pre-prod",
				},
			},
		},
	}

	if err := patchApplicationsHealth(context.Background(), &cfg, apps, "Suspended", "vCluster sleeping"); err != nil {
		t.Fatalf("unexpected error patching health: %v", err)
	}

	if len(patched) != 1 {
		t.Fatalf("expected exactly one health patch, got %d", len(patched))
	}
	if !strings.HasSuffix(patched[0], "/applications/plain-app/status") {
		t.Fatalf("expected only non-Kargo app to be patched, got %q", patched[0])
	}
}

func TestRestoreKargoApplicationsHealthUsesLastKnownHealthyStateWithDormantMessage(t *testing.T) {
	var patchedBodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Fatalf("expected PATCH request, got %s", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read patch body: %v", err)
		}
		patchedBodies = append(patchedBodies, string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      server.Client(),
			apiBase:     server.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace: "argocd",
		patchApplicationHealth:     true,
		applicationHealthPatchMode: applicationHealthPatchModeStatus,
		sleepingHealthMessage:      "vCluster sleeping",
		wakingHealthMessage:        "vCluster waking",
	}
	runtime := newWatcherRuntime()
	runtime.lastKnownKargoHealth["kargo-app"] = healthStatus{Status: "Healthy"}

	apps := []application{
		{
			Metadata: metadata{
				Name: "kargo-app",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "demo:pre-prod",
				},
			},
			Status: applicationStatus{
				Health: healthStatus{
					Status:  "Progressing",
					Message: "vCluster sleeping",
				},
			},
		},
	}

	if err := restoreKargoApplicationsHealth(context.Background(), &cfg, runtime, apps, "vCluster sleeping"); err != nil {
		t.Fatalf("unexpected restore error: %v", err)
	}

	if len(patchedBodies) != 1 {
		t.Fatalf("expected one Kargo health restore patch, got %d", len(patchedBodies))
	}
	if !strings.Contains(patchedBodies[0], `"status":"Healthy"`) {
		t.Fatalf("expected restore patch to set Healthy status, got %s", patchedBodies[0])
	}
	if !strings.Contains(patchedBodies[0], `"message":"vCluster sleeping"`) {
		t.Fatalf("expected restore patch to carry dormancy message, got %s", patchedBodies[0])
	}
}

func TestRestoreKargoApplicationsHealthSkipsActiveSyncIntent(t *testing.T) {
	var patched bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		patched = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      server.Client(),
			apiBase:     server.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace: "argocd",
		patchApplicationHealth:     true,
		applicationHealthPatchMode: applicationHealthPatchModeStatus,
		sleepingHealthMessage:      "vCluster sleeping",
		wakingHealthMessage:        "vCluster waking",
	}
	runtime := newWatcherRuntime()
	runtime.lastKnownKargoHealth["kargo-app"] = healthStatus{Status: "Healthy"}

	apps := []application{
		{
			Metadata: metadata{
				Name: "kargo-app",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "demo:pre-prod",
				},
			},
			Status: applicationStatus{
				Health: healthStatus{
					Status:  "Progressing",
					Message: "vCluster sleeping",
				},
			},
			Operation: &applicationOperation{
				Sync: json.RawMessage(`{"revision":"abc123"}`),
			},
		},
	}

	if err := restoreKargoApplicationsHealth(context.Background(), &cfg, runtime, apps, "vCluster sleeping"); err != nil {
		t.Fatalf("unexpected restore error: %v", err)
	}
	if patched {
		t.Fatal("did not expect Kargo health restore while sync intent is active")
	}
}

func TestRestoreKargoApplicationsHealthClearsDormantMessageWhenReady(t *testing.T) {
	var patchedBodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read patch body: %v", err)
		}
		patchedBodies = append(patchedBodies, string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      server.Client(),
			apiBase:     server.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace: "argocd",
		patchApplicationHealth:     true,
		applicationHealthPatchMode: applicationHealthPatchModeStatus,
		sleepingHealthMessage:      "vCluster sleeping",
		wakingHealthMessage:        "vCluster waking",
	}
	runtime := newWatcherRuntime()
	runtime.lastKnownKargoHealth["kargo-app"] = healthStatus{Status: "Healthy"}

	apps := []application{
		{
			Metadata: metadata{
				Name: "kargo-app",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "demo:pre-prod",
				},
			},
			Status: applicationStatus{
				Health: healthStatus{
					Status:  "Healthy",
					Message: "vCluster sleeping",
				},
			},
		},
	}

	if err := restoreKargoApplicationsHealth(context.Background(), &cfg, runtime, apps, ""); err != nil {
		t.Fatalf("unexpected restore error: %v", err)
	}

	if len(patchedBodies) != 1 {
		t.Fatalf("expected one ready-state restore patch, got %d", len(patchedBodies))
	}
	if !strings.Contains(patchedBodies[0], `"status":"Healthy"`) || !strings.Contains(patchedBodies[0], `"message":""`) {
		t.Fatalf("expected ready-state restore patch to clear dormancy message, got %s", patchedBodies[0])
	}
}

func TestRememberKargoApplicationsHealthSkipsTransientProgressingState(t *testing.T) {
	runtime := newWatcherRuntime()
	runtime.lastKnownKargoHealth["kargo-app"] = healthStatus{Status: "Healthy"}

	apps := []application{
		{
			Metadata: metadata{
				Name: "kargo-app",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "demo:pre-prod",
				},
			},
			Status: applicationStatus{
				Health: healthStatus{
					Status: "Progressing",
				},
			},
		},
	}

	rememberKargoApplicationsHealth(runtime, apps, watcherConfig{
		sleepingHealthMessage: "vCluster sleeping",
		wakingHealthMessage:   "vCluster waking",
	})

	if got := runtime.lastKnownKargoHealth["kargo-app"].Status; got != "Healthy" {
		t.Fatalf("expected cached healthy state to be preserved, got %q", got)
	}
}

func TestLoadWatcherConfigEnablesApplicationHealthPatchingByDefault(t *testing.T) {
	tokenPath := writeWatcherTestToken(t)

	t.Setenv("WATCH_KUBERNETES_API", "http://127.0.0.1")
	t.Setenv("WATCH_TOKEN_PATH", tokenPath)
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATE", "loft-{project}-vcluster-{virtualcluster}")
	t.Setenv("WATCH_PATCH_APPLICATION_HEALTH", "")

	cfg, err := loadWatcherConfig()
	if err != nil {
		t.Fatalf("unexpected error loading watcher config: %v", err)
	}
	if !cfg.patchApplicationHealth {
		t.Fatal("expected application health patching to default to enabled")
	}
}

func TestLoadWatcherConfigAllowsDisablingApplicationHealthPatching(t *testing.T) {
	tokenPath := writeWatcherTestToken(t)

	t.Setenv("WATCH_KUBERNETES_API", "http://127.0.0.1")
	t.Setenv("WATCH_TOKEN_PATH", tokenPath)
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATE", "loft-{project}-vcluster-{virtualcluster}")
	t.Setenv("WATCH_PATCH_APPLICATION_HEALTH", "false")

	cfg, err := loadWatcherConfig()
	if err != nil {
		t.Fatalf("unexpected error loading watcher config: %v", err)
	}
	if cfg.patchApplicationHealth {
		t.Fatal("expected application health patching to be disabled")
	}
}

func TestLoadWatcherConfigBuildsWakeRequesterWhenConfigured(t *testing.T) {
	tokenPath := writeWatcherTestToken(t)

	t.Setenv("WATCH_KUBERNETES_API", "http://127.0.0.1")
	t.Setenv("WATCH_TOKEN_PATH", tokenPath)
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATE", "loft-{project}-vcluster-{virtualcluster}")
	t.Setenv("WATCH_WAKE_UPSTREAM_BASE", "http://127.0.0.1")

	cfg, err := loadWatcherConfig()
	if err != nil {
		t.Fatalf("unexpected error loading watcher config: %v", err)
	}
	if cfg.wakeRequester == nil {
		t.Fatal("expected wake requester to be configured")
	}
	if cfg.wakeRetryInterval != defaultWakeRetryInterval {
		t.Fatalf("expected default wake retry interval %s, got %s", defaultWakeRetryInterval, cfg.wakeRetryInterval)
	}
}

func TestListApplicationsPaginatesResponsesLargerThanOneMiB(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Path != "/apis/argoproj.io/v1alpha1/namespaces/argocd/applications" {
			t.Fatalf("unexpected request path %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("limit"); got != "50" {
			t.Fatalf("expected page limit 50, got %q", got)
		}

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("continue") {
		case "":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"continue": "page-2"},
				"items": []any{map[string]any{
					"metadata": map[string]any{"name": "large-app"},
					"padding":  strings.Repeat("x", (1<<20)+1024),
				}},
			}); err != nil {
				t.Fatalf("encode first page: %v", err)
			}
		case "page-2":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"continue": ""},
				"items": []any{map[string]any{
					"metadata": map[string]any{"name": "small-app"},
				}},
			}); err != nil {
				t.Fatalf("encode second page: %v", err)
			}
		default:
			t.Fatalf("unexpected continue token %q", r.URL.Query().Get("continue"))
		}
	}))
	defer server.Close()

	api := &kubernetesAPI{client: server.Client(), apiBase: server.URL, bearerToken: "token"}
	apps, err := api.listApplications(context.Background(), "argocd")
	if err != nil {
		t.Fatalf("list applications: %v", err)
	}
	if requestCount != 2 {
		t.Fatalf("expected two paginated requests, got %d", requestCount)
	}
	if len(apps) != 2 || apps[0].Metadata.Name != "large-app" || apps[1].Metadata.Name != "small-app" {
		t.Fatalf("unexpected applications: %#v", apps)
	}
}

func TestReadResponseBodyReportsOverflow(t *testing.T) {
	_, err := readResponseBody(strings.NewReader("12345"), 4)
	if err == nil {
		t.Fatal("expected an explicit response-size error")
	}
	if !strings.Contains(err.Error(), "response body exceeds 4 bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplicationsByDestinationName(t *testing.T) {
	apps := []application{
		{
			Metadata: metadata{Name: "guestbook-dev"},
			Spec: applicationSpec{
				Destination: applicationDestination{Name: "loft-default-vcluster-pd-dev"},
			},
		},
		{
			Metadata: metadata{Name: "guestbook-pre-prod"},
			Spec: applicationSpec{
				Destination: applicationDestination{Name: "loft-default-vcluster-pre-prod-gate-pre-prod"},
			},
		},
		{
			Metadata: metadata{Name: "guestbook-pre-prod-copy"},
			Spec: applicationSpec{
				Destination: applicationDestination{Name: "loft-default-vcluster-pre-prod-gate-pre-prod"},
			},
		},
	}

	indexed := applicationsByDestinationName(apps)

	if got := len(indexed["loft-default-vcluster-pd-dev"]); got != 1 {
		t.Fatalf("expected 1 app for pd-dev destination, got %d", got)
	}
	if got := len(indexed["loft-default-vcluster-pre-prod-gate-pre-prod"]); got != 2 {
		t.Fatalf("expected 2 apps for pre-prod destination, got %d", got)
	}
}

func TestApplicationRefreshRequestFingerprint(t *testing.T) {
	app := application{
		Metadata: metadata{
			Name:            "guestbook-dev",
			ResourceVersion: "42",
			Annotations: map[string]string{
				argocdClusterRefreshAnnotation: "normal",
			},
		},
	}

	if got := applicationRefreshRequestFingerprint(app); got != "normal@42" {
		t.Fatalf("expected refresh fingerprint normal@42, got %q", got)
	}
}

func TestApplicationsByAuthorizedStage(t *testing.T) {
	apps := []application{
		{
			Metadata: metadata{
				Name: "guestbook-pre-prod",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "pre-prod-gate:pre-prod",
				},
			},
		},
		{
			Metadata: metadata{
				Name: "guestbook-prod",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "pre-prod-gate:prod",
				},
			},
		},
		{
			Metadata: metadata{
				Name: "plain-app",
			},
		},
	}

	indexed := applicationsByAuthorizedStage(apps)

	if got := len(indexed["pre-prod-gate/pre-prod"]); got != 1 {
		t.Fatalf("expected 1 app for pre-prod stage, got %d", got)
	}
	if got := len(indexed["pre-prod-gate/prod"]); got != 1 {
		t.Fatalf("expected 1 app for prod stage, got %d", got)
	}
	if _, ok := indexed[""]; ok {
		t.Fatal("did not expect empty stage key to be indexed")
	}
}

func TestClusterSecretNameTemplate(t *testing.T) {
	got := clusterSecretName("loft-{project}-vcluster-{virtualcluster}", "demo", "team-a")
	if got != "loft-demo-vcluster-team-a" {
		t.Fatalf("expected templated secret name, got %q", got)
	}
}

func TestKargoWakeTriggersByDestination(t *testing.T) {
	apps := []application{
		{
			Metadata: metadata{
				Name: "guestbook-pre-prod",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "pre-prod-gate:pre-prod",
				},
			},
			Spec: applicationSpec{
				Destination: applicationDestination{Name: "loft-default-vcluster-pre-prod-gate-pre-prod"},
			},
		},
	}

	promotions := []promotion{
		{
			Metadata: metadata{
				Name:      "pre-prod.abc123",
				Namespace: "pre-prod-gate",
			},
			Spec: promotionSpec{
				Stage: "pre-prod",
				Steps: []promotionStep{{Uses: "argocd-update"}},
			},
			Status: promotionStatus{Phase: "Running"},
		},
	}

	triggers := kargoWakeTriggersByDestination(apps, promotions)
	trigger, ok := triggers["loft-default-vcluster-pre-prod-gate-pre-prod"]
	if !ok {
		t.Fatal("expected Kargo wake trigger for destination")
	}
	if len(trigger.Apps) != 1 || trigger.Apps[0].Metadata.Name != "guestbook-pre-prod" {
		t.Fatalf("unexpected trigger apps: %#v", trigger.Apps)
	}
	if len(trigger.PromotionNames) != 1 || trigger.PromotionNames[0] != "pre-prod.abc123" {
		t.Fatalf("unexpected promotion names: %#v", trigger.PromotionNames)
	}
	if trigger.Fingerprint == "" {
		t.Fatal("expected non-empty trigger fingerprint")
	}
}

func TestKargoWakeTriggersIgnoreTerminalAndNonArgoPromotions(t *testing.T) {
	apps := []application{
		{
			Metadata: metadata{
				Name: "guestbook-pre-prod",
				Annotations: map[string]string{
					kargoAuthorizedStageAnnotation: "pre-prod-gate:pre-prod",
				},
			},
			Spec: applicationSpec{
				Destination: applicationDestination{Name: "loft-default-vcluster-pre-prod-gate-pre-prod"},
			},
		},
	}

	promotions := []promotion{
		{
			Metadata: metadata{Name: "pre-prod.done", Namespace: "pre-prod-gate"},
			Spec: promotionSpec{
				Stage: "pre-prod",
				Steps: []promotionStep{{Uses: "argocd-update"}},
			},
			Status: promotionStatus{Phase: "Succeeded"},
		},
		{
			Metadata: metadata{Name: "pre-prod.non-argocd", Namespace: "pre-prod-gate"},
			Spec: promotionSpec{
				Stage: "pre-prod",
				Steps: []promotionStep{{Uses: "git-clone"}},
			},
			Status: promotionStatus{Phase: "Running"},
		},
	}

	if triggers := kargoWakeTriggersByDestination(apps, promotions); len(triggers) != 0 {
		t.Fatalf("expected no wake triggers, got %#v", triggers)
	}
}

func TestReconcileVCITriggersWakeOncePerObservedSyncIntent(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET request to API server, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/secrets/"+secretName) {
			t.Fatalf("unexpected API path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST wake request, got %s", r.Method)
		}
		if r.URL.Path != "/kubernetes/project/demo/virtualcluster/team-a" {
			t.Fatalf("unexpected wake path %q", r.URL.Path)
		}
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		wakeRetryInterval:            time.Hour,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{Name: "guestbook-pre-prod"},
				Operation: &applicationOperation{
					Sync: json.RawMessage(`{"revision":"abc123"}`),
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected one wake call after new sync intent, got %d", wakeCalls)
	}

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error on second pass: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected wake call to be deduplicated, got %d calls", wakeCalls)
	}
}

func TestReconcileVCITriggersWakeOnNewRefreshRequest(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET request to API server, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/secrets/"+secretName) {
			t.Fatalf("unexpected API path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST wake request, got %s", r.Method)
		}
		if r.URL.Path != "/kubernetes/project/demo/virtualcluster/team-a" {
			t.Fatalf("unexpected wake path %q", r.URL.Path)
		}
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		wakeRetryInterval:            time.Hour,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{
					Name:            "guestbook-pre-prod",
					ResourceVersion: "2",
					Annotations: map[string]string{
						argocdClusterRefreshAnnotation: "normal",
					},
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected one wake call after new refresh request, got %d", wakeCalls)
	}

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error on second pass: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected refresh-triggered wake to be deduplicated, got %d calls", wakeCalls)
	}

	appsByDestination[secretName][0].Metadata.ResourceVersion = "3"
	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error on refreshed pass: %v", err)
	}
	if wakeCalls != 2 {
		t.Fatalf("expected a second wake call after a new refresh request, got %d", wakeCalls)
	}
}

func TestReconcileVCIHardRefreshesReadyAppsOnlyOncePerReadyTransition(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	var patched []string
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"annotations":{}}}`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/applications/guestbook-ready"):
			patched = append(patched, r.URL.Path)
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace:   "argocd",
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
		patchApplicationHealth:       true,
		sleepingHealthMessage:        "vCluster sleeping",
		wakingHealthMessage:          "vCluster waking",
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
		},
		Status: virtualClusterStatus{
			Phase: "Ready",
			Conditions: []condition{
				{Type: virtualClusterOnlineConditionType, Status: "True"},
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{Name: "guestbook-ready"},
				Status: applicationStatus{
					Health: healthStatus{
						Status:  "Healthy",
						Message: "vCluster sleeping",
					},
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", false)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if len(patched) != 1 {
		t.Fatalf("expected one hard refresh patch on first ready reconcile, got %d", len(patched))
	}

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error on second pass: %v", err)
	}
	if len(patched) != 1 {
		t.Fatalf("expected hard refresh to be deduplicated while still ready, got %d patches", len(patched))
	}

	vci.Status = virtualClusterStatus{}
	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error in unknown-state pass: %v", err)
	}

	vci.Status = virtualClusterStatus{
		Phase: "Ready",
		Conditions: []condition{
			{Type: virtualClusterOnlineConditionType, Status: "True"},
		},
	}
	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error after re-entering ready: %v", err)
	}
	if len(patched) != 2 {
		t.Fatalf("expected another hard refresh after a new ready transition, got %d patches", len(patched))
	}
}

func TestReconcileVCIRepausesIdleReadyClusterDespiteStaleRefreshAnnotation(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	var secretPatches int
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"annotations":{}}}`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			secretPatches++
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/applications/guestbook-ready"):
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace:   "argocd",
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
		patchApplicationHealth:       true,
		sleepingHealthMessage:        "vCluster sleeping",
		wakingHealthMessage:          "vCluster waking",
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
		},
		Status: virtualClusterStatus{
			Phase: "Ready",
			Conditions: []condition{
				{Type: virtualClusterOnlineConditionType, Status: "True"},
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{
					Name:            "guestbook-ready",
					ResourceVersion: "42",
					Annotations: map[string]string{
						argocdClusterRefreshAnnotation: "normal",
					},
				},
				Status: applicationStatus{
					Health: healthStatus{
						Status:  "Healthy",
						Message: "",
					},
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", false)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if secretPatches != 1 {
		t.Fatalf("expected one secret patch to re-pause idle ready cluster, got %d", secretPatches)
	}
}

func TestReconcileVCIRepausesIdleReadyClusterAfterOneManagedHealthRefresh(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	var secretPatches int
	var appPatches int
	secretPaused := false
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			w.Header().Set("Content-Type", "application/json")
			if secretPaused {
				_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
			} else {
				_, _ = w.Write([]byte(`{"metadata":{"annotations":{}}}`))
			}
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			secretPatches++
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read secret patch body: %v", err)
			}
			secretPaused = strings.Contains(string(body), `"argocd.argoproj.io/skip-reconcile":"true"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/applications/guestbook-ready"):
			appPatches++
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace:   "argocd",
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
		patchApplicationHealth:       true,
		sleepingHealthMessage:        "vCluster sleeping",
		wakingHealthMessage:          "vCluster waking",
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
		},
		Status: virtualClusterStatus{
			Phase: "Ready",
			Conditions: []condition{
				{Type: virtualClusterOnlineConditionType, Status: "True"},
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{Name: "guestbook-ready"},
				Status: applicationStatus{
					Health: healthStatus{
						Status:  "Healthy",
						Message: "vCluster sleeping",
					},
				},
			},
		},
	}

	// Rebuild the index each pass so it reflects the server-tracked pause state,
	// mirroring reconcileAll listing cluster Secrets fresh on every poll.
	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", secretPaused)}, nil)); err != nil {
		t.Fatalf("unexpected reconcile error on first ready pass: %v", err)
	}
	if appPatches != 1 {
		t.Fatalf("expected one application refresh patch on first ready pass, got %d", appPatches)
	}
	if secretPatches != 0 {
		t.Fatalf("expected no secret patch on first ready pass, got %d", secretPatches)
	}

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", secretPaused)}, nil)); err != nil {
		t.Fatalf("unexpected reconcile error on second ready pass: %v", err)
	}
	if appPatches != 1 {
		t.Fatalf("expected no extra application refresh patch on second ready pass, got %d", appPatches)
	}
	if secretPatches != 1 {
		t.Fatalf("expected one secret patch to re-pause idle ready cluster on second ready pass, got %d", secretPatches)
	}

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", secretPaused)}, nil)); err != nil {
		t.Fatalf("unexpected reconcile error on third ready pass: %v", err)
	}
	if appPatches != 1 {
		t.Fatalf("expected no extra application refresh patch on third ready pass, got %d", appPatches)
	}
	if secretPatches != 1 {
		t.Fatalf("expected paused idle ready cluster to stay paused on third ready pass, got %d secret patches", secretPatches)
	}
}

func TestReconcileVCIPausesUnknownStateClusterWhenIdle(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	var secretPatches int
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"annotations":{}}}`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			secretPatches++
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace:   "argocd",
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
		},
		Status: virtualClusterStatus{},
	}

	idx := reconcileIndexForTest(map[string][]application{secretName: nil}, []secret{clusterSecretForTest(secretName, "", false)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if secretPatches != 1 {
		t.Fatalf("expected one secret patch to pause unknown idle cluster, got %d", secretPatches)
	}
}

func TestReconcileVCIDoesNotRetryWakeFromStaleRefreshAnnotationAfterCooldown(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET request to API server, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/secrets/"+secretName) {
			t.Fatalf("unexpected API path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		wakeRetryInterval:            time.Second,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{
					Name:            "guestbook-pre-prod",
					ResourceVersion: "2",
					Annotations: map[string]string{
						argocdClusterRefreshAnnotation: "normal",
					},
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected one wake call after new refresh request, got %d", wakeCalls)
	}

	runtime.lastWakeAttempt[secretName] = time.Now().Add(-2 * time.Second)
	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error on stale refresh pass: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected no retry wake from stale refresh annotation, got %d calls", wakeCalls)
	}
}

func TestReconcileVCIRetriesWakeAfterCooldownWhenSyncIntentPersists(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		wakeRetryInterval:            time.Second,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	runtime.observedSyncIntents["guestbook-pre-prod"] = `{"revision":"abc123"}`
	runtime.lastWakeAttempt[secretName] = time.Now().Add(-2 * time.Second)

	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{Name: "guestbook-pre-prod"},
				Operation: &applicationOperation{
					Sync: json.RawMessage(`{"revision":"abc123"}`),
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected one retry wake call, got %d", wakeCalls)
	}
}

func TestReconcileVCITriggersWakeForNewOutOfSyncRevision(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET request to API server, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/secrets/"+secretName) {
			t.Fatalf("unexpected API path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST wake request, got %s", r.Method)
		}
		if r.URL.Path != "/kubernetes/project/demo/virtualcluster/team-a" {
			t.Fatalf("unexpected wake path %q", r.URL.Path)
		}
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		wakeRetryInterval:            time.Hour,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{Name: "guestbook-pre-prod"},
				Status: applicationStatus{
					Sync: applicationSync{
						Status:   "OutOfSync",
						Revision: "abc123",
					},
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected one wake call after new OutOfSync revision, got %d", wakeCalls)
	}

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error on second pass: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected OutOfSync revision wake to be deduplicated, got %d calls", wakeCalls)
	}
}

func TestReconcileVCIRetriesWakeAfterCooldownWhenOutOfSyncRevisionPersists(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		wakeRetryInterval:            time.Second,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	runtime.observedRevisionWakes["guestbook-pre-prod"] = "abc123"
	runtime.lastWakeAttempt[secretName] = time.Now().Add(-2 * time.Second)

	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{Name: "guestbook-pre-prod"},
				Status: applicationStatus{
					Sync: applicationSync{
						Status:   "OutOfSync",
						Revision: "abc123",
					},
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected one retry wake call for persistent OutOfSync revision, got %d", wakeCalls)
	}
}

func TestReconcileVCIUpdatesVCILastActivityOnWakeWhenEnabled(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	const vciStatusPath = "/apis/management.loft.sh/v1/namespaces/p-demo/virtualclusterinstances/team-a/status"

	var patchBodies []string
	before := time.Now().Unix()
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
		case r.Method == http.MethodPatch && r.URL.Path == vciStatusPath:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read status patch body: %v", err)
			}
			patchBodies = append(patchBodies, string(body))
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected API request %s %q", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		updateVCILastActivityOnWake:  true,
		wakeRetryInterval:            time.Hour,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{Name: "guestbook-pre-prod"},
				Operation: &applicationOperation{
					Sync: json.RawMessage(`{"revision":"abc123"}`),
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected one wake call, got %d", wakeCalls)
	}
	if len(patchBodies) != 1 {
		t.Fatalf("expected one VCI status patch, got %d", len(patchBodies))
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(patchBodies[0]), &payload); err != nil {
		t.Fatalf("unmarshal status patch: %v", err)
	}
	status, _ := payload["status"].(map[string]any)
	sleepModeConfig, _ := status["sleepModeConfig"].(map[string]any)
	sleepStatus, _ := sleepModeConfig["status"].(map[string]any)
	lastActivity, _ := sleepStatus["lastActivity"].(float64)
	after := time.Now().Unix()
	if int64(lastActivity) < before || int64(lastActivity) > after {
		t.Fatalf("expected lastActivity to be patched to a current timestamp between %d and %d, got %v", before, after, lastActivity)
	}
}

func TestReconcileVCIIgnoresVCILastActivityPatchFailure(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"
	const vciStatusPath = "/apis/management.loft.sh/v1/namespaces/p-demo/virtualclusterinstances/team-a/status"

	patchCalls := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/secrets/"+secretName):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
		case r.Method == http.MethodPatch && r.URL.Path == vciStatusPath:
			patchCalls++
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"forbidden"}`))
		default:
			t.Fatalf("unexpected API request %s %q", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		updateVCILastActivityOnWake:  true,
		wakeRetryInterval:            time.Hour,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{Name: "guestbook-pre-prod"},
				Operation: &applicationOperation{
					Sync: json.RawMessage(`{"revision":"abc123"}`),
				},
			},
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, nil)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected one wake call, got %d", wakeCalls)
	}
	if patchCalls != 1 {
		t.Fatalf("expected one best-effort VCI status patch attempt, got %d", patchCalls)
	}
}

func TestReconcileVCITriggersWakeForActiveKargoPromotionBeforeSyncIntent(t *testing.T) {
	const secretName = "loft-demo-vcluster-team-a"

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"annotations":{"argocd.argoproj.io/skip-reconcile":"true"}}}`))
	}))
	defer apiServer.Close()

	wakeCalls := 0
	wakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakeCalls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer wakeServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		wakeRequester: &wakeRequester{
			client:           wakeServer.Client(),
			baseURL:          wakeServer.URL,
			acceptedStatuses: parseStatusSet("502,504"),
		},
		wakeRetryInterval:            time.Hour,
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   []string{"loft-{project}-vcluster-{virtualcluster}"},
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}

	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}
	appsByDestination := map[string][]application{
		secretName: {
			{
				Metadata: metadata{
					Name: "guestbook-pre-prod",
					Annotations: map[string]string{
						kargoAuthorizedStageAnnotation: "pre-prod-gate:pre-prod",
					},
				},
			},
		},
	}
	kargoWakeTriggers := map[string]kargoWakeTrigger{
		secretName: {
			Apps: []application{
				{
					Metadata: metadata{Name: "guestbook-pre-prod"},
				},
			},
			PromotionNames: []string{"pre-prod.abc123"},
			Fingerprint:    "pre-prod.abc123",
		},
	}

	idx := reconcileIndexForTest(appsByDestination, []secret{clusterSecretForTest(secretName, "", true)}, kargoWakeTriggers)

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("expected wake call from active Kargo Promotion, got %d", wakeCalls)
	}
	if got := runtime.observedKargoPromotions[secretName]; got != "pre-prod.abc123" {
		t.Fatalf("expected observed Kargo promotion fingerprint to be remembered, got %q", got)
	}
}

func TestListPromotionsOptionalTreatsMissingKargoAsUnsupported(t *testing.T) {
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/kargo.akuity.io/v1alpha1/promotions" {
			t.Fatalf("unexpected API path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"the server could not find the requested resource"}`))
	}))
	defer apiServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
	}
	runtime := newWatcherRuntime()

	promotions, err := listPromotionsOptional(context.Background(), &cfg, runtime)
	if err != nil {
		t.Fatalf("unexpected error listing optional promotions: %v", err)
	}
	if len(promotions) != 0 {
		t.Fatalf("expected no promotions when Kargo is unavailable, got %#v", promotions)
	}
	if !runtime.kargoPromotionsChecked || runtime.kargoPromotionsAvailable {
		t.Fatalf("expected Kargo promotion discovery to be marked unavailable, got checked=%v available=%v", runtime.kargoPromotionsChecked, runtime.kargoPromotionsAvailable)
	}
}

func writeWatcherTestToken(t *testing.T) string {
	t.Helper()

	tokenFile, err := os.CreateTemp(t.TempDir(), "watcher-token-*")
	if err != nil {
		t.Fatalf("create temp token file: %v", err)
	}
	if _, err := tokenFile.WriteString("test-token\n"); err != nil {
		t.Fatalf("write temp token file: %v", err)
	}
	if err := tokenFile.Close(); err != nil {
		t.Fatalf("close temp token file: %v", err)
	}

	return tokenFile.Name()
}

func b64(v string) string {
	return base64.StdEncoding.EncodeToString([]byte(v))
}

// v2ClusterSecret builds an Argo CD v2 cluster Secret: auto-generated
// metadata.name, but data.name/data.server carrying the platform cluster name
// and tenant server URL.
func v2ClusterSecret(metaName, dataName, server string) secret {
	data := map[string]string{}
	if dataName != "" {
		data["name"] = b64(dataName)
	}
	if server != "" {
		data["server"] = b64(server)
	}
	return secret{Metadata: metadata{Name: metaName}, Data: data}
}

func TestClusterSecretIndexResolvesV2VirtualclusterInfixByDataName(t *testing.T) {
	const dataName = "loft-demo-virtualcluster-team-a"
	secrets := []secret{
		v2ClusterSecret("cluster-host-abc123", dataName, "https://team-a.platform.example.com"),
	}
	index := buildClusterSecretIndex(secrets)

	templates := parseList(defaultClusterSecretNameTemplates)
	expected := expandClusterSecretNames(templates, "demo", "team-a")

	resolved := index.resolve(expected, nil)
	if resolved.secret == nil {
		t.Fatalf("expected to resolve v2 secret by data.name, got nil")
	}
	if resolved.secretMetadataName() != "cluster-host-abc123" {
		t.Fatalf("expected real metadata.name to be used for patching, got %q", resolved.secretMetadataName())
	}
	if resolved.clusterName != dataName {
		t.Fatalf("expected resolved cluster name %q, got %q", dataName, resolved.clusterName)
	}
	if resolved.server != "https://team-a.platform.example.com" {
		t.Fatalf("expected resolved server URL, got %q", resolved.server)
	}
}

func TestClusterSecretIndexResolvesV2ArgocdSuffix(t *testing.T) {
	// The v2 connector appends "-argocd" to the registered cluster name, e.g.
	// vCluster "llm-large" in project "default".
	const dataName = "loft-default-virtualcluster-llm-large-argocd"
	secrets := []secret{
		v2ClusterSecret("cluster-host-xyz", dataName, "https://llm-large.platform.example.com"),
	}
	index := buildClusterSecretIndex(secrets)

	templates := parseList(defaultClusterSecretNameTemplates)
	expected := expandClusterSecretNames(templates, "default", "llm-large")

	resolved := index.resolve(expected, nil)
	if resolved.secret == nil {
		t.Fatalf("expected to resolve v2 secret with -argocd suffix, got nil")
	}
	if resolved.secretMetadataName() != "cluster-host-xyz" {
		t.Fatalf("expected real metadata.name for patching, got %q", resolved.secretMetadataName())
	}
	if resolved.clusterName != dataName {
		t.Fatalf("expected resolved cluster name %q, got %q", dataName, resolved.clusterName)
	}
	if resolved.matchedViaPrefix {
		t.Fatalf("expected an exact data.name match, not a prefix fallback")
	}
}

func TestClusterSecretIndexResolvesLegacyVclusterInfixByDataName(t *testing.T) {
	const dataName = "loft-demo-vcluster-team-a"
	secrets := []secret{
		// v1: metadata.name equals data.name.
		clusterSecretForTest(dataName, "https://team-a.platform.example.com", false),
	}
	index := buildClusterSecretIndex(secrets)

	templates := parseList(defaultClusterSecretNameTemplates)
	expected := expandClusterSecretNames(templates, "demo", "team-a")

	resolved := index.resolve(expected, nil)
	if resolved.secret == nil || resolved.clusterName != dataName {
		t.Fatalf("expected to resolve legacy secret by data.name, got %+v", resolved)
	}
}

func TestClusterSecretIndexResolvesByServerWhenOnlyServerMatches(t *testing.T) {
	const server = "https://team-a.platform.example.com"
	secrets := []secret{
		// data.name does not match any template; only the server URL lines up.
		v2ClusterSecret("cluster-host-zzz", "some-unexpected-cluster-name", server),
	}
	index := buildClusterSecretIndex(secrets)

	templates := parseList(defaultClusterSecretNameTemplates)
	expected := expandClusterSecretNames(templates, "demo", "team-a")

	resolved := index.resolve(expected, []string{server + "/"})
	if resolved.secret == nil {
		t.Fatalf("expected to resolve secret by server URL, got nil")
	}
	if resolved.secretMetadataName() != "cluster-host-zzz" {
		t.Fatalf("expected metadata.name from server match, got %q", resolved.secretMetadataName())
	}
}

func TestClusterSecretIndexNotFoundReturnsPrimaryExpectedName(t *testing.T) {
	index := buildClusterSecretIndex(nil)
	templates := parseList(defaultClusterSecretNameTemplates)
	expected := expandClusterSecretNames(templates, "demo", "team-a")

	resolved := index.resolve(expected, nil)
	if resolved.secret != nil {
		t.Fatalf("expected no secret, got %+v", resolved.secret)
	}
	if resolved.secretMetadataName() != "" {
		t.Fatalf("expected empty metadata.name when unresolved, got %q", resolved.secretMetadataName())
	}
	if resolved.clusterName != expected[0] {
		t.Fatalf("expected fallback runtime key %q, got %q", expected[0], resolved.clusterName)
	}
}

func TestClusterSecretIndexResolvesViaTruncatedPrefix(t *testing.T) {
	longProject := strings.Repeat("p", 40)
	templates := parseList(defaultClusterSecretNameTemplates)
	expected := expandClusterSecretNames(templates, longProject, "team-a")

	full := expected[0]
	if len(full) <= clusterNameMaxLength {
		t.Fatalf("test setup expected a name longer than %d, got %d", clusterNameMaxLength, len(full))
	}
	// Simulate the platform's SafeConcatNameMax truncation with a hash suffix.
	truncated := full[:clusterNamePrefixMatchLength] + "-9f2a1"

	index := buildClusterSecretIndex([]secret{v2ClusterSecret("cluster-host-trunc", truncated, "https://x")})

	resolved := index.resolve(expected, nil)
	if resolved.secret == nil || !resolved.matchedViaPrefix {
		t.Fatalf("expected prefix-fallback match, got %+v", resolved)
	}
	if resolved.clusterName != truncated {
		t.Fatalf("expected resolved cluster name %q, got %q", truncated, resolved.clusterName)
	}
}

func TestApplicationsByDestinationServerIndexesByNormalizedServer(t *testing.T) {
	apps := []application{
		{
			Metadata: metadata{Name: "guestbook-dev"},
			Spec:     applicationSpec{Destination: applicationDestination{Server: "https://Team-A.Platform.Example.com/"}},
		},
		{
			Metadata: metadata{Name: "akuity-app"},
			Spec:     applicationSpec{Destination: applicationDestination{Name: "loft-demo-virtualcluster-team-a"}},
		},
	}

	byServer := applicationsByDestinationServer(apps)
	if got := len(byServer["https://team-a.platform.example.com"]); got != 1 {
		t.Fatalf("expected one app indexed by normalized server, got %d", got)
	}
	// The Akuity app (name only, no server) must not be indexed by server.
	if len(byServer) != 1 {
		t.Fatalf("expected exactly one server key, got %d", len(byServer))
	}
}

func TestNormalizeServerURLEquality(t *testing.T) {
	tests := []struct {
		left  string
		right string
	}{
		{"https://Example.com/", "https://example.com"},
		{"HTTPS://EXAMPLE.COM:443/", "https://example.com:443"},
		{"https://example.com/path/", "https://example.com/path"},
	}
	for _, tt := range tests {
		if normalizeServerURL(tt.left) != normalizeServerURL(tt.right) {
			t.Fatalf("expected %q and %q to normalize equal, got %q vs %q", tt.left, tt.right, normalizeServerURL(tt.left), normalizeServerURL(tt.right))
		}
	}
}

func TestReconcileVCIMatchesPlainArgoCDV2ApplicationByServer(t *testing.T) {
	const (
		metaName = "cluster-host-abc123"
		dataName = "loft-demo-virtualcluster-team-a"
		server   = "https://team-a.platform.example.com"
	)

	var patchedSecrets []string
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/secrets/"+metaName):
			patchedSecrets = append(patchedSecrets, r.URL.Path)
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace:   "argocd",
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   parseList(defaultClusterSecretNameTemplates),
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}

	// Plain Argo CD v2: Application targets the tenant cluster by server, not name.
	app := application{
		Metadata: metadata{Name: "guestbook"},
		Spec:     applicationSpec{Destination: applicationDestination{Server: server}},
	}
	idx := reconcileIndex{
		appsByDestinationName:   map[string][]application{},
		appsByDestinationServer: applicationsByDestinationServer([]application{app}),
		clusterSecrets:          buildClusterSecretIndex([]secret{v2ClusterSecret(metaName, dataName, server)}),
		kargoWakeTriggers:       map[string]kargoWakeTrigger{},
	}

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if len(patchedSecrets) != 1 {
		t.Fatalf("expected the auto-named v2 secret to be paused once, got %d patches", len(patchedSecrets))
	}
}

func TestReconcileVCIMatchesAkuityV2ApplicationByName(t *testing.T) {
	const (
		metaName = "cluster-host-akuity"
		dataName = "loft-demo-virtualcluster-team-a"
	)

	patchedSecrets := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/secrets/"+metaName) {
			patchedSecrets++
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
	}))
	defer apiServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace:   "argocd",
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   parseList(defaultClusterSecretNameTemplates),
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}

	// Akuity v2: Application targets the tenant cluster by name.
	app := application{
		Metadata: metadata{Name: "guestbook"},
		Spec:     applicationSpec{Destination: applicationDestination{Name: dataName}},
	}
	idx := reconcileIndex{
		appsByDestinationName:   applicationsByDestinationName([]application{app}),
		appsByDestinationServer: map[string][]application{},
		clusterSecrets:          buildClusterSecretIndex([]secret{v2ClusterSecret(metaName, dataName, "")}),
		kargoWakeTriggers:       map[string]kargoWakeTrigger{},
	}

	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if patchedSecrets != 1 {
		t.Fatalf("expected the Akuity v2 secret to be paused once, got %d patches", patchedSecrets)
	}
}

func TestReconcileVCIWithoutPatchableSecretDoesNotClaimPause(t *testing.T) {
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("did not expect any API call, got %s %s", r.Method, r.URL.Path)
	}))
	defer apiServer.Close()

	cfg := watcherConfig{
		api: &kubernetesAPI{
			client:      apiServer.Client(),
			apiBase:     apiServer.URL,
			bearerToken: "token",
		},
		argocdApplicationNamespace:   "argocd",
		argocdClusterSecretNamespace: "argocd",
		clusterSecretNameTemplates:   parseList(defaultClusterSecretNameTemplates),
		projectNamespacePrefixes:     []string{"p-", "loft-p-"},
		patchApplicationHealth:       true,
		sleepingHealthMessage:        "vCluster sleeping",
		wakingHealthMessage:          "vCluster waking",
	}
	runtime := newWatcherRuntime()
	vci := virtualClusterInstance{
		Metadata: metadata{
			Name:      "team-a",
			Namespace: "p-demo",
			Annotations: map[string]string{
				sleepingSinceAnnotation: "1711800000",
			},
		},
	}

	// Discovery-only entry (no metadata.name) e.g. from the Argo CD REST API.
	index := buildClusterSecretIndex(nil)
	index.addDiscoveryCluster("loft-demo-virtualcluster-team-a", "https://team-a.platform.example.com")
	idx := reconcileIndex{
		appsByDestinationName:   map[string][]application{},
		appsByDestinationServer: map[string][]application{},
		clusterSecrets:          index,
		kargoWakeTriggers:       map[string]kargoWakeTrigger{},
	}

	// No secret patch and no health patch must be attempted (apiServer t.Fatalf
	// guards this). Pause is disabled because the Secret is not patchable.
	if err := reconcileVCI(context.Background(), &cfg, runtime, vci, idx); err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
}

func TestLoadWatcherConfigDefaultsToBothTemplates(t *testing.T) {
	tokenPath := writeWatcherTestToken(t)

	t.Setenv("WATCH_KUBERNETES_API", "http://127.0.0.1")
	t.Setenv("WATCH_TOKEN_PATH", tokenPath)
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATE", "")
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATES", "")

	cfg, err := loadWatcherConfig()
	if err != nil {
		t.Fatalf("unexpected error loading watcher config: %v", err)
	}
	want := []string{
		"loft-{project}-virtualcluster-{virtualcluster}",
		"loft-{project}-vcluster-{virtualcluster}",
	}
	if len(cfg.clusterSecretNameTemplates) != len(want) {
		t.Fatalf("expected default templates %v, got %v", want, cfg.clusterSecretNameTemplates)
	}
	for i := range want {
		if cfg.clusterSecretNameTemplates[i] != want[i] {
			t.Fatalf("expected template %q at %d, got %q", want[i], i, cfg.clusterSecretNameTemplates[i])
		}
	}
}

func TestLoadWatcherConfigSingularTemplateStillPinsSingle(t *testing.T) {
	tokenPath := writeWatcherTestToken(t)

	t.Setenv("WATCH_KUBERNETES_API", "http://127.0.0.1")
	t.Setenv("WATCH_TOKEN_PATH", tokenPath)
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATE", "loft-{project}-vcluster-{virtualcluster}")
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATES", "")

	cfg, err := loadWatcherConfig()
	if err != nil {
		t.Fatalf("unexpected error loading watcher config: %v", err)
	}
	if len(cfg.clusterSecretNameTemplates) != 1 || cfg.clusterSecretNameTemplates[0] != "loft-{project}-vcluster-{virtualcluster}" {
		t.Fatalf("expected singular template to pin a single entry, got %v", cfg.clusterSecretNameTemplates)
	}
}

func TestLoadWatcherConfigPluralTemplatesSupersedeSingular(t *testing.T) {
	tokenPath := writeWatcherTestToken(t)

	t.Setenv("WATCH_KUBERNETES_API", "http://127.0.0.1")
	t.Setenv("WATCH_TOKEN_PATH", tokenPath)
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATE", "loft-{project}-vcluster-{virtualcluster}")
	t.Setenv("ARGOCD_CLUSTER_SECRET_NAME_TEMPLATES", "a-{project}-{virtualcluster},b-{project}-{virtualcluster}")

	cfg, err := loadWatcherConfig()
	if err != nil {
		t.Fatalf("unexpected error loading watcher config: %v", err)
	}
	if len(cfg.clusterSecretNameTemplates) != 2 || cfg.clusterSecretNameTemplates[0] != "a-{project}-{virtualcluster}" {
		t.Fatalf("expected plural templates to win, got %v", cfg.clusterSecretNameTemplates)
	}
}
