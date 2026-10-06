package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
)

const (
	defaultWakeAccessKeyName = "vcluster-gitops-watcher-wake"

	accessKeyManagedByLabel = "app.kubernetes.io/managed-by"
	accessKeyManagedByValue = "vcluster-gitops-watcher"
	// sleepModeIgnoreActivityLabel makes the platform drop a key's requests from
	// sleep mode, including the wake path, so a wake key must never carry it.
	sleepModeIgnoreActivityLabel = "sleepmode.loft.sh/ignore-activity"

	accessKeyTypeUser   = "User"
	accessKeyKeyLength  = 64
	accessKeyKeyCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	storageAPIPath = "/apis/storage.loft.sh/v1"
)

// wakeAccessKeyConfig describes the platform AccessKey the watcher creates and
// keeps in shape for its own wake requests.
type wakeAccessKeyConfig struct {
	name     string
	user     string
	team     string
	projects []string
}

// storageAccessKey is the subset of storage.loft.sh/v1 AccessKey the watcher
// reads and writes.
type storageAccessKey struct {
	APIVersion string               `json:"apiVersion,omitempty"`
	Kind       string               `json:"kind,omitempty"`
	Metadata   metadata             `json:"metadata"`
	Spec       storageAccessKeySpec `json:"spec"`
}

type storageAccessKeySpec struct {
	DisplayName string `json:"displayName,omitempty"`
	Description string `json:"description,omitempty"`
	User        string `json:"user,omitempty"`
	Team        string `json:"team,omitempty"`
	Key         string `json:"key,omitempty"`
	Disabled    bool   `json:"disabled,omitempty"`
	Type        string `json:"type,omitempty"`
	// Scope is kept raw so fields the watcher does not manage are still seen
	// when deciding whether the key has drifted.
	Scope json.RawMessage `json:"scope,omitempty"`
}

type accessKeyScope struct {
	Projects []accessKeyScopeProject `json:"projects,omitempty"`
}

type accessKeyScopeProject struct {
	Project string `json:"project"`
}

// loadWakeAccessKeyConfig reads the WATCH_WAKE_ACCESS_KEY_* variables. It
// returns nil when neither a user nor a team is set, which leaves access key
// management off.
func loadWakeAccessKeyConfig() (*wakeAccessKeyConfig, error) {
	user := strings.TrimSpace(os.Getenv("WATCH_WAKE_ACCESS_KEY_USER"))
	team := strings.TrimSpace(os.Getenv("WATCH_WAKE_ACCESS_KEY_TEAM"))
	if user == "" && team == "" {
		return nil, nil
	}
	if user != "" && team != "" {
		return nil, errors.New("set only one of WATCH_WAKE_ACCESS_KEY_USER or WATCH_WAKE_ACCESS_KEY_TEAM")
	}

	projects := parseList(mustEnv("WATCH_WAKE_ACCESS_KEY_PROJECTS", "*"))
	if len(projects) == 0 {
		return nil, errors.New("WATCH_WAKE_ACCESS_KEY_PROJECTS must contain at least one project or *")
	}

	return &wakeAccessKeyConfig{
		name:     mustEnv("WATCH_WAKE_ACCESS_KEY_NAME", defaultWakeAccessKeyName),
		user:     user,
		team:     team,
		projects: projects,
	}, nil
}

// wakeAccessKeyManager creates the wake AccessKey on first use and caches its
// key. A refresh re-reads and repairs the AccessKey, for example after a 401.
type wakeAccessKeyManager struct {
	api *kubernetesAPI
	cfg wakeAccessKeyConfig

	mu  sync.Mutex
	key string
}

func newWakeAccessKeyManager(api *kubernetesAPI, cfg wakeAccessKeyConfig) *wakeAccessKeyManager {
	return &wakeAccessKeyManager{api: api, cfg: cfg}
}

// token returns the bearer token for wake requests, ensuring the AccessKey
// when nothing is cached yet or refresh is true.
func (m *wakeAccessKeyManager) token(ctx context.Context, refresh bool) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.key != "" && !refresh {
		return m.key, nil
	}
	key, err := m.ensure(ctx)
	if err != nil {
		return "", err
	}
	m.key = key
	return key, nil
}

func (m *wakeAccessKeyManager) ensure(ctx context.Context) (string, error) {
	if err := m.checkOwner(ctx); err != nil {
		return "", err
	}

	path := storageAPIPath + "/accesskeys/" + url.PathEscape(m.cfg.name)
	var existing storageAccessKey
	err := m.api.getJSON(ctx, path, nil, &existing)
	switch {
	case isAPIStatus(err, http.StatusNotFound):
		key, err := generateAccessKeyValue()
		if err != nil {
			return "", err
		}
		body, err := json.Marshal(m.desired(key))
		if err != nil {
			return "", err
		}
		if _, err := m.api.request(ctx, http.MethodPost, storageAPIPath+"/accesskeys", nil, "application/json", body); err != nil {
			return "", fmt.Errorf("create wake access key %s: %w", m.cfg.name, err)
		}
		log.Printf("created vCluster Platform access key %s for %s to send wake requests", m.cfg.name, m.ownerDescription())
		return key, nil
	case err != nil:
		return "", fmt.Errorf("get wake access key %s: %w", m.cfg.name, err)
	}

	if existing.Metadata.Labels[accessKeyManagedByLabel] != accessKeyManagedByValue {
		return "", fmt.Errorf("access key %s already exists and is not managed by %s; set WATCH_WAKE_ACCESS_KEY_NAME to another name", m.cfg.name, accessKeyManagedByValue)
	}

	key := existing.Spec.Key
	if key == "" {
		if key, err = generateAccessKeyValue(); err != nil {
			return "", err
		}
	}
	if m.drifted(existing, key) {
		if err := m.api.mergePatch(ctx, path, m.repairPatch(key)); err != nil {
			return "", fmt.Errorf("repair wake access key %s: %w", m.cfg.name, err)
		}
		log.Printf("repaired vCluster Platform access key %s for %s", m.cfg.name, m.ownerDescription())
	}
	return key, nil
}

// checkOwner fails early when the configured user or team does not exist,
// since the platform rejects keys without a valid owner. A forbidden lookup is
// only logged, so the check stays optional in tighter RBAC setups.
func (m *wakeAccessKeyManager) checkOwner(ctx context.Context) error {
	resource, name := "users", m.cfg.user
	if name == "" {
		resource, name = "teams", m.cfg.team
	}
	var owner struct {
		Metadata metadata `json:"metadata"`
	}
	err := m.api.getJSON(ctx, storageAPIPath+"/"+resource+"/"+url.PathEscape(name), nil, &owner)
	switch {
	case err == nil:
		return nil
	case isAPIStatus(err, http.StatusNotFound):
		return fmt.Errorf("vCluster Platform %s %q for the wake access key does not exist", strings.TrimSuffix(resource, "s"), name)
	case isAPIStatus(err, http.StatusForbidden):
		log.Printf("cannot verify vCluster Platform %s %q for the wake access key (forbidden); continuing", strings.TrimSuffix(resource, "s"), name)
		return nil
	default:
		return fmt.Errorf("look up vCluster Platform %s %q: %w", strings.TrimSuffix(resource, "s"), name, err)
	}
}

func (m *wakeAccessKeyManager) ownerDescription() string {
	if m.cfg.user != "" {
		return "user " + m.cfg.user
	}
	return "team " + m.cfg.team
}

func (m *wakeAccessKeyManager) scope() accessKeyScope {
	scope := accessKeyScope{}
	for _, project := range m.cfg.projects {
		scope.Projects = append(scope.Projects, accessKeyScopeProject{Project: project})
	}
	return scope
}

func (m *wakeAccessKeyManager) desired(key string) storageAccessKey {
	scope, _ := json.Marshal(m.scope())
	return storageAccessKey{
		APIVersion: "storage.loft.sh/v1",
		Kind:       "AccessKey",
		Metadata: metadata{
			Name:   m.cfg.name,
			Labels: map[string]string{accessKeyManagedByLabel: accessKeyManagedByValue},
		},
		Spec: storageAccessKeySpec{
			DisplayName: "vcluster-gitops-watcher wake",
			Description: "Created by vcluster-gitops-watcher to wake sleeping tenant clusters on GitOps intent.",
			User:        m.cfg.user,
			Team:        m.cfg.team,
			Key:         key,
			Type:        accessKeyTypeUser,
			Scope:       scope,
		},
	}
}

// drifted reports whether the AccessKey no longer matches what the watcher
// needs: wrong owner or scope, disabled, missing key, or carrying the
// ignore-activity label that would stop it from waking tenant clusters.
func (m *wakeAccessKeyManager) drifted(existing storageAccessKey, key string) bool {
	if existing.Spec.Key != key || existing.Spec.Disabled ||
		existing.Spec.User != m.cfg.user || existing.Spec.Team != m.cfg.team {
		return true
	}
	if _, ok := existing.Metadata.Labels[sleepModeIgnoreActivityLabel]; ok {
		return true
	}

	var raw map[string]json.RawMessage
	if len(existing.Spec.Scope) > 0 && json.Unmarshal(existing.Spec.Scope, &raw) != nil {
		return true
	}
	for field := range raw {
		if field != "projects" {
			return true
		}
	}
	var current accessKeyScope
	if len(existing.Spec.Scope) > 0 && json.Unmarshal(existing.Spec.Scope, &current) != nil {
		return true
	}
	return !slices.Equal(current.Projects, m.scope().Projects)
}

// repairPatch is a JSON merge patch that resets every field the watcher
// manages, clearing the other owner field and any scope fields it does not set.
func (m *wakeAccessKeyManager) repairPatch(key string) map[string]any {
	owner := map[string]any{"user": nil, "team": nil}
	if m.cfg.user != "" {
		owner["user"] = m.cfg.user
	} else {
		owner["team"] = m.cfg.team
	}
	spec := map[string]any{
		"key":      key,
		"disabled": nil,
		"type":     accessKeyTypeUser,
		"scope": map[string]any{
			"projects":        m.scope().Projects,
			"roles":           nil,
			"spaces":          nil,
			"virtualClusters": nil,
			"clusters":        nil,
			"rules":           nil,
			"allowLoftCli":    nil,
		},
	}
	for k, v := range owner {
		spec[k] = v
	}
	return map[string]any{
		"metadata": map[string]any{
			"labels": map[string]any{sleepModeIgnoreActivityLabel: nil},
		},
		"spec": spec,
	}
}

func generateAccessKeyValue() (string, error) {
	// Rejection sampling keeps every character equally likely.
	limit := byte(256 - 256%len(accessKeyKeyCharset))
	out := make([]byte, 0, accessKeyKeyLength)
	buf := make([]byte, accessKeyKeyLength)
	for len(out) < accessKeyKeyLength {
		if _, err := rand.Read(buf); err != nil {
			return "", fmt.Errorf("generate access key: %w", err)
		}
		for _, b := range buf {
			if b >= limit {
				continue
			}
			out = append(out, accessKeyKeyCharset[int(b)%len(accessKeyKeyCharset)])
			if len(out) == accessKeyKeyLength {
				break
			}
		}
	}
	return string(out), nil
}

func isAPIStatus(err error, statusCode int) bool {
	var statusErr *apiStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == statusCode
}
