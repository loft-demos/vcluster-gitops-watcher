package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultWakeTokenTTL = 10 * time.Minute
	minWakeTokenTTL     = time.Minute
	// minWakeTokenRenewMargin keeps a cached token from being used right up
	// to its expiry.
	minWakeTokenRenewMargin = 30 * time.Second
)

// wakeIdentity is the vCluster Platform user or team the watcher impersonates
// to get short-lived wake tokens. Exactly one of user and team is set.
type wakeIdentity struct {
	user string
	team string
}

// loadWakeIdentity reads WATCH_WAKE_ACCESS_KEY_USER / _TEAM. It returns nil
// when neither is set, which leaves impersonated wake tokens off.
func loadWakeIdentity() (*wakeIdentity, error) {
	user := strings.TrimSpace(os.Getenv("WATCH_WAKE_ACCESS_KEY_USER"))
	team := strings.TrimSpace(os.Getenv("WATCH_WAKE_ACCESS_KEY_TEAM"))
	if user == "" && team == "" {
		return nil, nil
	}
	if user != "" && team != "" {
		return nil, errors.New("set only one of WATCH_WAKE_ACCESS_KEY_USER or WATCH_WAKE_ACCESS_KEY_TEAM")
	}
	for _, retired := range []string{"WATCH_WAKE_ACCESS_KEY_NAME", "WATCH_WAKE_ACCESS_KEY_PROJECTS"} {
		if strings.TrimSpace(os.Getenv(retired)) != "" {
			log.Printf("%s is no longer used: wake tokens are now scoped to one tenant cluster each", retired)
		}
	}
	return &wakeIdentity{user: user, team: team}, nil
}

// impersonation returns the Impersonate-User and Impersonate-Group values
// vCluster Platform expects for this user or team.
func (i wakeIdentity) impersonation() (string, string) {
	if i.user != "" {
		return i.user, "loft:user:" + i.user
	}
	return "loft:team:" + i.team, "loft:team:" + i.team
}

// activitySubject is the sleep-mode activity subject of requests made with
// tokens issued for this identity.
func (i wakeIdentity) activitySubject() string {
	if i.user != "" {
		return "loft:user:" + i.user
	}
	return "loft:team:" + i.team
}

func (i wakeIdentity) String() string {
	if i.user != "" {
		return "user " + i.user
	}
	return "team " + i.team
}

type cachedWakeToken struct {
	token   string
	expires time.Time
}

// wakeTokenIssuer gets a short-lived, single-tenant-cluster token for wake
// requests by asking vCluster Platform for a token kubeconfig while
// impersonating the wake identity. Platform creates and later garbage
// collects the AccessKey behind each token, so the watcher never needs
// permission to create AccessKeys itself.
type wakeTokenIssuer struct {
	api      *kubernetesAPI
	identity wakeIdentity
	ttl      time.Duration
	now      func() time.Time

	mu    sync.Mutex
	cache map[string]cachedWakeToken
}

func newWakeTokenIssuer(api *kubernetesAPI, identity wakeIdentity, ttl time.Duration) *wakeTokenIssuer {
	if ttl <= 0 {
		ttl = defaultWakeTokenTTL
	}
	return &wakeTokenIssuer{api: api, identity: identity, ttl: ttl, now: time.Now, cache: map[string]cachedWakeToken{}}
}

func (t *wakeTokenIssuer) renewMargin() time.Duration {
	margin := t.ttl / 5
	if margin < minWakeTokenRenewMargin {
		margin = minWakeTokenRenewMargin
	}
	return margin
}

// token returns a token for the VCI, reusing a cached one until shortly
// before it expires. refresh forces a new token, for example after a 401.
func (t *wakeTokenIssuer) token(ctx context.Context, namespace, name string, refresh bool) (string, error) {
	key := namespace + "/" + name
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	if cached, ok := t.cache[key]; ok && !refresh && now.Before(cached.expires.Add(-t.renewMargin())) {
		return cached.token, nil
	}
	token, err := t.issue(ctx, namespace, name)
	if err != nil {
		delete(t.cache, key)
		return "", err
	}
	t.cache[key] = cachedWakeToken{token: token, expires: now.Add(t.ttl)}
	return token, nil
}

func (t *wakeTokenIssuer) issue(ctx context.Context, namespace, name string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"apiVersion": "management.loft.sh/v1",
		"kind":       "VirtualClusterInstanceKubeConfig",
		"metadata":   map[string]any{"namespace": namespace},
		// A token kubeconfig, not a client certificate: Platform creates an
		// AccessKey scoped to this VCI with certificateTTL as its TTL.
		"spec": map[string]any{"certificateTTL": int64(t.ttl.Seconds()), "clientCert": false},
	})
	if err != nil {
		return "", err
	}

	impersonateUser, impersonateGroup := t.identity.impersonation()
	headers := http.Header{}
	headers.Set("Impersonate-User", impersonateUser)
	headers.Add("Impersonate-Group", impersonateGroup)

	path := "/apis/management.loft.sh/v1/namespaces/" + url.PathEscape(namespace) + "/virtualclusterinstances/" + url.PathEscape(name) + "/kubeconfig"
	data, err := t.api.requestWithHeaders(ctx, http.MethodPost, path, nil, "application/json", body, headers)
	if err != nil {
		return "", fmt.Errorf("get wake token for %s/%s as %s: %w", namespace, name, t.identity, err)
	}

	var result struct {
		Status struct {
			KubeConfig string `json:"kubeConfig"`
		} `json:"status"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return "", fmt.Errorf("decode wake token kubeconfig for %s/%s: %w", namespace, name, err)
	}
	token, err := kubeconfigToken(result.Status.KubeConfig)
	if err != nil {
		return "", fmt.Errorf("wake token kubeconfig for %s/%s: %w", namespace, name, err)
	}
	return token, nil
}

// kubeconfigToken extracts the bearer token from a single-user kubeconfig
// without a YAML dependency. Platform writes it as a "token:" line under
// users[0].user.
func kubeconfigToken(kubeconfig string) (string, error) {
	if strings.TrimSpace(kubeconfig) == "" {
		return "", errors.New("kubeconfig is empty")
	}
	for _, line := range strings.Split(kubeconfig, "\n") {
		value, ok := strings.CutPrefix(strings.TrimSpace(line), "token:")
		if !ok {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if value != "" {
			return value, nil
		}
	}
	return "", errors.New("kubeconfig has no token; client certificates are not supported for wake requests")
}

func isAPIStatus(err error, statusCode int) bool {
	var statusErr *apiStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == statusCode
}
