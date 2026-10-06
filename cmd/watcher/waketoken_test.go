package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeKubeconfigAPI answers VirtualClusterInstanceKubeConfig requests with a
// fresh token each time and records what it was asked for.
type fakeKubeconfigAPI struct {
	mu       sync.Mutex
	issued   []string
	paths    []string
	users    []string
	groups   []string
	ttls     []float64
	failWith int
}

func (f *fakeKubeconfigAPI) server(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/kubeconfig") {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if f.failWith != 0 {
			w.WriteHeader(f.failWith)
			_, _ = w.Write([]byte(`{"kind":"Status","message":"forbidden"}`))
			return
		}
		var body struct {
			Spec struct {
				CertificateTTL float64 `json:"certificateTTL"`
				ClientCert     bool    `json:"clientCert"`
			} `json:"spec"`
		}
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &body)
		if body.Spec.ClientCert {
			t.Errorf("wake tokens must request a token kubeconfig, not a client certificate")
		}
		token := "token-" + strconv.Itoa(len(f.issued)+1)
		f.issued = append(f.issued, token)
		f.paths = append(f.paths, r.URL.Path)
		f.users = append(f.users, r.Header.Get("Impersonate-User"))
		f.groups = append(f.groups, r.Header.Get("Impersonate-Group"))
		f.ttls = append(f.ttls, body.Spec.CertificateTTL)
		kubeconfig := "apiVersion: v1\nclusters:\n- cluster:\n    server: https://platform.example.com/kubernetes/project/demo/virtualcluster/team-a\n  name: c\nusers:\n- name: u\n  user:\n    token: " + token + "\n"
		_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"kubeConfig": kubeconfig}})
	}))
	t.Cleanup(server.Close)
	return server
}

func newTestWakeTokenIssuer(t *testing.T, fake *fakeKubeconfigAPI, identity wakeIdentity, ttl time.Duration) *wakeTokenIssuer {
	server := fake.server(t)
	return newWakeTokenIssuer(&kubernetesAPI{client: server.Client(), apiBase: server.URL, bearerToken: "sa"}, identity, ttl)
}

func TestWakeTokenIssuerImpersonatesUserAndCachesPerVCI(t *testing.T) {
	fake := &fakeKubeconfigAPI{}
	issuer := newTestWakeTokenIssuer(t, fake, wakeIdentity{user: "gitops-watcher"}, 10*time.Minute)
	now := time.Now()
	issuer.now = func() time.Time { return now }

	first, err := issuer.token(context.Background(), "p-demo", "team-a", false)
	if err != nil || first != "token-1" {
		t.Fatalf("got %q, %v", first, err)
	}
	if fake.paths[0] != "/apis/management.loft.sh/v1/namespaces/p-demo/virtualclusterinstances/team-a/kubeconfig" {
		t.Fatalf("unexpected path %q", fake.paths[0])
	}
	if fake.users[0] != "gitops-watcher" || fake.groups[0] != "loft:user:gitops-watcher" {
		t.Fatalf("unexpected impersonation %q / %q", fake.users[0], fake.groups[0])
	}
	if fake.ttls[0] != 600 {
		t.Fatalf("expected certificateTTL 600, got %v", fake.ttls[0])
	}

	// Cached for the same VCI, separate for another VCI.
	if again, _ := issuer.token(context.Background(), "p-demo", "team-a", false); again != "token-1" {
		t.Fatalf("expected the cached token, got %q", again)
	}
	if other, _ := issuer.token(context.Background(), "p-demo", "team-b", false); other != "token-2" {
		t.Fatalf("expected a separate token for another VCI, got %q", other)
	}

	// Renewed once inside the renew margin before expiry.
	now = now.Add(9 * time.Minute)
	if renewed, _ := issuer.token(context.Background(), "p-demo", "team-a", false); renewed != "token-3" {
		t.Fatalf("expected a renewed token near expiry, got %q", renewed)
	}

	// Forced refresh always issues a new token.
	if refreshed, _ := issuer.token(context.Background(), "p-demo", "team-a", true); refreshed != "token-4" {
		t.Fatalf("expected a forced refresh, got %q", refreshed)
	}
}

func TestWakeTokenIssuerImpersonatesTeam(t *testing.T) {
	fake := &fakeKubeconfigAPI{}
	issuer := newTestWakeTokenIssuer(t, fake, wakeIdentity{team: "platform"}, 5*time.Minute)
	if _, err := issuer.token(context.Background(), "p-demo", "team-a", false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.users[0] != "loft:team:platform" || fake.groups[0] != "loft:team:platform" {
		t.Fatalf("unexpected team impersonation %q / %q", fake.users[0], fake.groups[0])
	}
}

func TestWakeTokenIssuerReportsForbidden(t *testing.T) {
	fake := &fakeKubeconfigAPI{failWith: http.StatusForbidden}
	issuer := newTestWakeTokenIssuer(t, fake, wakeIdentity{user: "gitops-watcher"}, 10*time.Minute)
	_, err := issuer.token(context.Background(), "p-demo", "team-a", false)
	if err == nil || !strings.Contains(err.Error(), "user gitops-watcher") || !isAPIStatus(err, http.StatusForbidden) {
		t.Fatalf("expected a forbidden error naming the identity, got %v", err)
	}
}

func TestWakeRequesterRetriesWithFreshTokenOn401(t *testing.T) {
	fake := &fakeKubeconfigAPI{}
	issuer := newTestWakeTokenIssuer(t, fake, wakeIdentity{user: "gitops-watcher"}, 10*time.Minute)

	var authHeaders []string
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		if r.URL.Path != "/kubernetes/project/demo/virtualcluster/team-a/version" {
			t.Errorf("unexpected wake path %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") == "Bearer token-1" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer platform.Close()

	requester := &wakeRequester{client: platform.Client(), baseURL: platform.URL, tokens: issuer}
	if err := requester.Execute(context.Background(), "demo", "p-demo", "team-a"); err != nil {
		t.Fatalf("expected the wake to succeed after a token refresh, got %v", err)
	}
	if len(authHeaders) != 2 || authHeaders[1] != "Bearer token-2" {
		t.Fatalf("expected a retry with a fresh token, got %v", authHeaders)
	}
}

func TestKubeconfigToken(t *testing.T) {
	token, err := kubeconfigToken("users:\n- name: u\n  user:\n    token: \"abc123\"\n")
	if err != nil || token != "abc123" {
		t.Fatalf("got %q, %v", token, err)
	}
	if _, err := kubeconfigToken("users:\n- name: u\n  user:\n    client-certificate-data: x\n"); err == nil {
		t.Fatalf("expected an error for a kubeconfig without a token")
	}
	if _, err := kubeconfigToken(""); err == nil {
		t.Fatalf("expected an error for an empty kubeconfig")
	}
}

func TestLoadWatcherConfigWakeIdentity(t *testing.T) {
	t.Setenv("WATCH_KUBERNETES_API", "http://127.0.0.1")
	t.Setenv("WATCH_TOKEN_PATH", writeWatcherTestToken(t))
	t.Setenv("WATCH_WAKE_UPSTREAM_BASE", "http://platform.example.com")
	t.Setenv("WATCH_WAKE_ACCESS_KEY_USER", "gitops-watcher")

	t.Setenv("WATCH_WAKE_BEARER_TOKEN", "static")
	if _, err := loadWatcherConfig(); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("expected a conflict error, got %v", err)
	}
	t.Setenv("WATCH_WAKE_BEARER_TOKEN", "")

	t.Setenv("WATCH_WAKE_TOKEN_TTL", "30s")
	if _, err := loadWatcherConfig(); err == nil {
		t.Fatalf("expected a TTL below the minimum to be rejected")
	}
	t.Setenv("WATCH_WAKE_TOKEN_TTL", "5m")

	cfg, err := loadWatcherConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.wakeRequester.tokens == nil || cfg.wakeRequester.tokens.ttl != 5*time.Minute {
		t.Fatalf("expected an issuer with a 5m TTL, got %+v", cfg.wakeRequester.tokens)
	}
	if cfg.wakeSubject != "loft:user:gitops-watcher" {
		t.Fatalf("expected the wake subject to be derived, got %q", cfg.wakeSubject)
	}

	t.Setenv("WATCH_WAKE_ACCESS_KEY_TEAM", "platform")
	if _, err := loadWatcherConfig(); err == nil {
		t.Fatalf("expected an error when both user and team are set")
	}
}

func TestWakeRequesterTreatsClientTimeoutAsTriggered(t *testing.T) {
	release := make(chan struct{})
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Platform holds the wake request while the tenant cluster starts.
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer platform.Close()
	defer close(release)

	client := platform.Client()
	client.Timeout = 50 * time.Millisecond
	requester := &wakeRequester{client: client, baseURL: platform.URL, bearerToken: "static"}
	if err := requester.Execute(context.Background(), "demo", "p-demo", "team-a"); err != nil {
		t.Fatalf("expected a client timeout to count as a triggered wake, got %v", err)
	}
}

func TestWakeRequesterReportsConnectionFailure(t *testing.T) {
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	baseURL := platform.URL
	platform.Close()

	requester := &wakeRequester{client: &http.Client{Timeout: time.Second}, baseURL: baseURL, bearerToken: "static"}
	if err := requester.Execute(context.Background(), "demo", "p-demo", "team-a"); err == nil {
		t.Fatalf("expected a refused connection to be reported as an error")
	}
}
