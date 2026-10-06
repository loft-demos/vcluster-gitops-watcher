package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeAccessKeyAPI serves users, teams, and one AccessKey the way the
// storage.loft.sh API would, and records every write.
type fakeAccessKeyAPI struct {
	mu        sync.Mutex
	users     map[string]bool
	accessKey map[string]any
	creates   []map[string]any
	patches   []map[string]any
}

func (f *fakeAccessKeyAPI) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, storageAPIPath+"/users/"):
			if !f.users[strings.TrimPrefix(r.URL.Path, storageAPIPath+"/users/")] {
				http.Error(w, `{"reason":"NotFound"}`, http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"metadata":{}}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, storageAPIPath+"/accesskeys/"):
			if f.accessKey == nil {
				http.Error(w, `{"reason":"NotFound"}`, http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(f.accessKey)
		case r.Method == http.MethodPost && r.URL.Path == storageAPIPath+"/accesskeys":
			body := decodeBody(t, r)
			f.creates = append(f.creates, body)
			f.accessKey = body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(body)
		case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, storageAPIPath+"/accesskeys/"):
			f.patches = append(f.patches, decodeBody(t, r))
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func decodeBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	data, _ := io.ReadAll(r.Body)
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	return out
}

func newTestAccessKeyManager(t *testing.T, fake *fakeAccessKeyAPI, cfg wakeAccessKeyConfig) *wakeAccessKeyManager {
	t.Helper()
	server := httptest.NewServer(fake.handler(t))
	t.Cleanup(server.Close)
	if cfg.name == "" {
		cfg.name = defaultWakeAccessKeyName
	}
	if len(cfg.projects) == 0 {
		cfg.projects = []string{"*"}
	}
	return newWakeAccessKeyManager(&kubernetesAPI{client: server.Client(), apiBase: server.URL, bearerToken: "sa"}, cfg)
}

func TestWakeAccessKeyCreatedWhenMissing(t *testing.T) {
	fake := &fakeAccessKeyAPI{users: map[string]bool{"gitops-watcher": true}}
	manager := newTestAccessKeyManager(t, fake, wakeAccessKeyConfig{user: "gitops-watcher"})

	key, err := manager.token(context.Background(), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(key) != accessKeyKeyLength {
		t.Fatalf("expected a %d-character key, got %d", accessKeyKeyLength, len(key))
	}
	if len(fake.creates) != 1 {
		t.Fatalf("expected one create, got %d", len(fake.creates))
	}

	created := fake.creates[0]
	meta := created["metadata"].(map[string]any)
	labels := meta["labels"].(map[string]any)
	if labels[accessKeyManagedByLabel] != accessKeyManagedByValue {
		t.Fatalf("expected managed-by label, got %v", labels)
	}
	if _, ok := labels[sleepModeIgnoreActivityLabel]; ok {
		t.Fatalf("wake key must not carry %s", sleepModeIgnoreActivityLabel)
	}
	spec := created["spec"].(map[string]any)
	if spec["user"] != "gitops-watcher" || spec["key"] != key || spec["type"] != accessKeyTypeUser {
		t.Fatalf("unexpected spec %v", spec)
	}
	projects := spec["scope"].(map[string]any)["projects"].([]any)
	if len(projects) != 1 || projects[0].(map[string]any)["project"] != "*" {
		t.Fatalf("expected scope projects [*], got %v", projects)
	}

	// The key is cached: no further API writes on the next call.
	again, err := manager.token(context.Background(), false)
	if err != nil || again != key {
		t.Fatalf("expected cached key, got %q, %v", again, err)
	}
	if len(fake.creates) != 1 {
		t.Fatalf("expected no second create, got %d", len(fake.creates))
	}
}

func managedAccessKeyFixture(user, key string) map[string]any {
	return map[string]any{
		"metadata": map[string]any{
			"name":   defaultWakeAccessKeyName,
			"labels": map[string]any{accessKeyManagedByLabel: accessKeyManagedByValue},
		},
		"spec": map[string]any{
			"user":  user,
			"key":   key,
			"type":  accessKeyTypeUser,
			"scope": map[string]any{"projects": []any{map[string]any{"project": "*"}}},
		},
	}
}

func TestWakeAccessKeyReusesHealthyExistingKey(t *testing.T) {
	fake := &fakeAccessKeyAPI{
		users:     map[string]bool{"gitops-watcher": true},
		accessKey: managedAccessKeyFixture("gitops-watcher", "existing-key"),
	}
	manager := newTestAccessKeyManager(t, fake, wakeAccessKeyConfig{user: "gitops-watcher"})

	key, err := manager.token(context.Background(), false)
	if err != nil || key != "existing-key" {
		t.Fatalf("expected existing key, got %q, %v", key, err)
	}
	if len(fake.creates)+len(fake.patches) != 0 {
		t.Fatalf("expected no writes, got %d creates and %d patches", len(fake.creates), len(fake.patches))
	}
}

func TestWakeAccessKeyRepairsDrift(t *testing.T) {
	existing := managedAccessKeyFixture("someone-else", "existing-key")
	existing["metadata"].(map[string]any)["labels"].(map[string]any)[sleepModeIgnoreActivityLabel] = "true"
	spec := existing["spec"].(map[string]any)
	spec["disabled"] = true
	spec["scope"].(map[string]any)["rules"] = []any{map[string]any{"verbs": []any{"get"}}}

	fake := &fakeAccessKeyAPI{users: map[string]bool{"gitops-watcher": true}, accessKey: existing}
	manager := newTestAccessKeyManager(t, fake, wakeAccessKeyConfig{user: "gitops-watcher"})

	key, err := manager.token(context.Background(), false)
	if err != nil || key != "existing-key" {
		t.Fatalf("expected the existing key to be kept, got %q, %v", key, err)
	}
	if len(fake.patches) != 1 {
		t.Fatalf("expected one repair patch, got %d", len(fake.patches))
	}

	patch := fake.patches[0]
	labels := patch["metadata"].(map[string]any)["labels"].(map[string]any)
	if v, ok := labels[sleepModeIgnoreActivityLabel]; !ok || v != nil {
		t.Fatalf("expected the ignore-activity label to be removed, got %v", labels)
	}
	patchSpec := patch["spec"].(map[string]any)
	if patchSpec["user"] != "gitops-watcher" || patchSpec["disabled"] != nil {
		t.Fatalf("unexpected repaired spec %v", patchSpec)
	}
	if v, ok := patchSpec["scope"].(map[string]any)["rules"]; !ok || v != nil {
		t.Fatalf("expected unmanaged scope fields to be cleared, got %v", patchSpec["scope"])
	}
}

func TestWakeAccessKeyRefusesUnmanagedKey(t *testing.T) {
	existing := managedAccessKeyFixture("gitops-watcher", "someone-elses-key")
	existing["metadata"].(map[string]any)["labels"] = map[string]any{}
	fake := &fakeAccessKeyAPI{users: map[string]bool{"gitops-watcher": true}, accessKey: existing}
	manager := newTestAccessKeyManager(t, fake, wakeAccessKeyConfig{user: "gitops-watcher"})

	if _, err := manager.token(context.Background(), false); err == nil || !strings.Contains(err.Error(), "not managed by") {
		t.Fatalf("expected an adoption error, got %v", err)
	}
	if len(fake.patches) != 0 {
		t.Fatalf("an unmanaged key must never be patched")
	}
}

func TestWakeAccessKeyFailsForMissingUser(t *testing.T) {
	fake := &fakeAccessKeyAPI{users: map[string]bool{}}
	manager := newTestAccessKeyManager(t, fake, wakeAccessKeyConfig{user: "nobody"})

	if _, err := manager.token(context.Background(), false); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected a missing-user error, got %v", err)
	}
	if len(fake.creates) != 0 {
		t.Fatalf("expected no create for a missing user")
	}
}

func TestWakeRequesterRefreshesManagedKeyOn401(t *testing.T) {
	fake := &fakeAccessKeyAPI{
		users:     map[string]bool{"gitops-watcher": true},
		accessKey: managedAccessKeyFixture("gitops-watcher", "stale-key"),
	}
	manager := newTestAccessKeyManager(t, fake, wakeAccessKeyConfig{user: "gitops-watcher"})
	if _, err := manager.token(context.Background(), false); err != nil {
		t.Fatalf("prime token: %v", err)
	}
	// Someone deleted the key; the next ensure creates a new one.
	fake.mu.Lock()
	fake.accessKey = nil
	fake.mu.Unlock()

	var authHeaders []string
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		if r.Header.Get("Authorization") == "Bearer stale-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer platform.Close()

	requester := &wakeRequester{client: platform.Client(), baseURL: platform.URL, accessKey: manager}
	if err := requester.Execute(context.Background(), "demo", "team-a"); err != nil {
		t.Fatalf("expected the wake to succeed after a key refresh, got %v", err)
	}
	if len(authHeaders) != 2 || authHeaders[0] != "Bearer stale-key" || authHeaders[1] == "Bearer stale-key" {
		t.Fatalf("expected a retry with a fresh key, got %v", authHeaders)
	}
	if len(fake.creates) != 1 {
		t.Fatalf("expected the deleted key to be recreated, got %d creates", len(fake.creates))
	}
}

func TestLoadWakeAccessKeyConfig(t *testing.T) {
	t.Setenv("WATCH_WAKE_ACCESS_KEY_USER", "")
	t.Setenv("WATCH_WAKE_ACCESS_KEY_TEAM", "")
	if cfg, err := loadWakeAccessKeyConfig(); cfg != nil || err != nil {
		t.Fatalf("expected management off by default, got %+v, %v", cfg, err)
	}

	t.Setenv("WATCH_WAKE_ACCESS_KEY_USER", "a")
	t.Setenv("WATCH_WAKE_ACCESS_KEY_TEAM", "b")
	if _, err := loadWakeAccessKeyConfig(); err == nil {
		t.Fatalf("expected an error when both user and team are set")
	}

	t.Setenv("WATCH_WAKE_ACCESS_KEY_TEAM", "")
	t.Setenv("WATCH_WAKE_ACCESS_KEY_PROJECTS", "demo, prod")
	cfg, err := loadWakeAccessKeyConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.name != defaultWakeAccessKeyName || cfg.user != "a" || strings.Join(cfg.projects, ",") != "demo,prod" {
		t.Fatalf("unexpected config %+v", cfg)
	}
}

func TestLoadWatcherConfigRejectsAccessKeyWithStaticToken(t *testing.T) {
	t.Setenv("WATCH_KUBERNETES_API", "http://127.0.0.1")
	t.Setenv("WATCH_TOKEN_PATH", writeWatcherTestToken(t))
	t.Setenv("WATCH_WAKE_UPSTREAM_BASE", "http://platform.example.com")
	t.Setenv("WATCH_WAKE_BEARER_TOKEN", "static")
	t.Setenv("WATCH_WAKE_ACCESS_KEY_USER", "gitops-watcher")

	if _, err := loadWatcherConfig(); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("expected a conflict error, got %v", err)
	}

	t.Setenv("WATCH_WAKE_BEARER_TOKEN", "")
	cfg, err := loadWatcherConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.wakeRequester == nil || cfg.wakeRequester.accessKey == nil {
		t.Fatalf("expected the wake requester to use the managed access key")
	}
}

func TestGenerateAccessKeyValue(t *testing.T) {
	key, err := generateAccessKeyValue()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(key) != accessKeyKeyLength || strings.Trim(key, accessKeyKeyCharset) != "" {
		t.Fatalf("unexpected key %q", key)
	}
	other, _ := generateAccessKeyValue()
	if key == other {
		t.Fatalf("expected distinct keys")
	}
}
