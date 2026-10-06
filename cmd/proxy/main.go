// cmd/proxy/main.go
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
)

const (
	argocdClusterRefreshAnnotation = "argocd.argoproj.io/refresh"

	defaultKubernetesServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	defaultKubernetesServiceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type wakeRequestInfo struct {
	Project        string
	VirtualCluster string
	UpstreamURL    string
	ForwardHeaders http.Header
}

type wakeAcceptedAction interface {
	Execute(ctx context.Context, wake wakeRequestInfo) error
}

type argoClusterSecretRefreshAction struct {
	client             *http.Client
	apiBase            string
	bearerToken        string
	namespace          string
	secretName         string
	secretNameTemplate string
}

type readinessWaitAction struct {
	client    *http.Client
	timeout   time.Duration
	interval  time.Duration
	checkPath string
}

type compositeWakeAcceptedAction struct {
	actions []wakeAcceptedAction
}

func mustEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseOverrideSet(v string) map[int]struct{} {
	set := map[int]struct{}{}
	if v == "" {
		return set
	}
	for _, s := range strings.Split(v, ",") {
		switch strings.TrimSpace(s) {
		case "502":
			set[502] = struct{}{}
		case "504":
			set[504] = struct{}{}
		case "500":
			set[500] = struct{}{}
		case "429":
			set[429] = struct{}{}
		}
	}
	return set
}

func parseList(v string) []string {
	if v == "" {
		return nil
	}

	var items []string
	for _, s := range strings.Split(v, ",") {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" {
			continue
		}
		items = append(items, s)
	}
	return items
}

func shouldDumpRequest(r *http.Request, skippedUserAgents []string) bool {
	if len(skippedUserAgents) == 0 {
		return true
	}

	userAgent := strings.ToLower(strings.TrimSpace(r.UserAgent()))
	for _, prefix := range skippedUserAgents {
		if strings.HasPrefix(userAgent, prefix) {
			return false
		}
	}
	return true
}

func cloneForwardHeaders(src http.Header) http.Header {
	dst := make(http.Header, len(src))
	hopByHop := map[string]struct{}{
		"Connection":          {},
		"Proxy-Connection":    {},
		"Keep-Alive":          {},
		"Proxy-Authenticate":  {},
		"Proxy-Authorization": {},
		"Te":                  {},
		"Trailer":             {},
		"Transfer-Encoding":   {},
		"Upgrade":             {},
	}

	for k, vv := range src {
		if _, skip := hopByHop[http.CanonicalHeaderKey(k)]; skip {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}

	return dst
}

// parseWakeRequest recognizes the two wake request shapes: a POST under the
// tenant cluster path, and GET <base>/version, which vcluster-gitops-watcher
// sends because any authenticated user may read /version once the woken
// cluster forwards the request.
func parseWakeRequest(r *http.Request) (wakeRequestInfo, bool) {
	info, ok := parseWakePath(r.URL.Path)
	if !ok {
		return wakeRequestInfo{}, false
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.Method == http.MethodPost:
		return info, true
	case r.Method == http.MethodGet && len(parts) == 6 && parts[5] == "version":
		return info, true
	default:
		return wakeRequestInfo{}, false
	}
}

func parseWakePath(path string) (wakeRequestInfo, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 5 {
		return wakeRequestInfo{}, false
	}

	if parts[0] != "kubernetes" ||
		parts[1] != "project" ||
		parts[2] == "" ||
		parts[3] != "virtualcluster" ||
		parts[4] == "" {
		return wakeRequestInfo{}, false
	}

	return wakeRequestInfo{
		Project:        parts[2],
		VirtualCluster: parts[4],
	}, true
}

func isWakePath(path string) bool {
	_, ok := parseWakePath(path)
	return ok
}

func inClusterKubernetesAPIBase() (string, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	if host == "" {
		return "", errors.New("KUBERNETES_SERVICE_HOST is not set")
	}

	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS"))
	if port == "" {
		port = strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	}
	if port == "" {
		port = "443"
	}

	return "https://" + net.JoinHostPort(host, port), nil
}

func newClusterRefreshHTTPClient(apiBase, caPath string, timeout time.Duration) (*http.Client, error) {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       30 * time.Second,
	}

	if strings.HasPrefix(strings.ToLower(apiBase), "https://") {
		caPEM, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read cluster CA %q: %w", caPath, err)
		}

		rootCAs, err := x509.SystemCertPool()
		if err != nil || rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}
		if ok := rootCAs.AppendCertsFromPEM(caPEM); !ok {
			return nil, fmt.Errorf("load cluster CA from %q", caPath)
		}

		transport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
		}
	}

	return &http.Client{Transport: transport, Timeout: timeout}, nil
}

func (a compositeWakeAcceptedAction) Execute(ctx context.Context, wake wakeRequestInfo) error {
	for _, action := range a.actions {
		if err := action.Execute(ctx, wake); err != nil {
			return err
		}
	}
	return nil
}

func isRetryableReadinessStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func (a *readinessWaitAction) readinessURL(wake wakeRequestInfo) string {
	return strings.TrimRight(wake.UpstreamURL, "/") + "/" + strings.TrimLeft(a.checkPath, "/")
}

func (a *readinessWaitAction) Execute(ctx context.Context, wake wakeRequestInfo) error {
	if a == nil || a.timeout <= 0 {
		return nil
	}

	readinessURL := a.readinessURL(wake)
	deadline := time.Now().Add(a.timeout)
	startedAt := time.Now()
	attempts := 0
	var lastErr error

	for {
		attempts++

		attemptCtx, cancel := context.WithDeadline(ctx, deadline)
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, readinessURL, nil)
		if err != nil {
			cancel()
			return fmt.Errorf("build readiness request: %w", err)
		}
		req.Header = cloneForwardHeaders(wake.ForwardHeaders)

		resp, err := a.client.Do(req)
		cancel()
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
				log.Printf("wake readiness check succeeded for project=%s virtualcluster=%s after %d attempt(s) in %s via %s", wake.Project, wake.VirtualCluster, attempts, time.Since(startedAt).Round(time.Millisecond), readinessURL)
				return nil
			}

			lastErr = fmt.Errorf("readiness probe %s returned %s", readinessURL, resp.Status)
			if !isRetryableReadinessStatus(resp.StatusCode) {
				return lastErr
			}
		} else {
			lastErr = fmt.Errorf("readiness probe %s failed: %w", readinessURL, err)
			if !isRetryableWakeError(err) && !errors.Is(err, context.DeadlineExceeded) {
				return lastErr
			}
		}

		if time.Now().After(deadline) {
			break
		}

		wait := a.interval
		if wait <= 0 {
			wait = 2 * time.Second
		}
		if remaining := time.Until(deadline); wait > remaining {
			wait = remaining
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	if lastErr == nil {
		lastErr = errors.New("no readiness response received")
	}
	return fmt.Errorf("cluster did not become ready within %s: %w", a.timeout, lastErr)
}

func (a *argoClusterSecretRefreshAction) targetSecretName(wake wakeRequestInfo) string {
	if a.secretName != "" {
		return a.secretName
	}

	replacer := strings.NewReplacer(
		"{project}", wake.Project,
		"{virtualcluster}", wake.VirtualCluster,
	)
	return replacer.Replace(a.secretNameTemplate)
}

func (a *argoClusterSecretRefreshAction) Execute(ctx context.Context, wake wakeRequestInfo) error {
	secretName := strings.TrimSpace(a.targetSecretName(wake))
	if secretName == "" {
		return errors.New("target cluster secret name resolved to empty string")
	}

	payload, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				argocdClusterRefreshAnnotation: time.Now().UTC().Format(time.RFC3339Nano),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal cluster refresh patch: %w", err)
	}

	targetURL := strings.TrimRight(a.apiBase, "/") +
		"/api/v1/namespaces/" + url.PathEscape(a.namespace) +
		"/secrets/" + url.PathEscape(secretName)

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, targetURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build cluster refresh request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+a.bearerToken)
	req.Header.Set("Content-Type", "application/merge-patch+json")
	req.Header.Set("Accept", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("patch cluster secret %s/%s: %w", a.namespace, secretName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("patch cluster secret %s/%s: %s: %s", a.namespace, secretName, resp.Status, strings.TrimSpace(string(body)))
	}

	log.Printf("refreshed Argo cluster cache hint via secret %s/%s for project=%s virtualcluster=%s", a.namespace, secretName, wake.Project, wake.VirtualCluster)
	return nil
}

func newReadinessWaitActionFromEnv(client *http.Client) (wakeAcceptedAction, error) {
	timeoutValue := strings.TrimSpace(os.Getenv("WAKE_READY_TIMEOUT"))
	if timeoutValue == "" {
		return nil, nil
	}

	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil {
		return nil, fmt.Errorf("parse WAKE_READY_TIMEOUT: %w", err)
	}
	if timeout <= 0 {
		return nil, nil
	}

	interval := 2 * time.Second
	if v := strings.TrimSpace(os.Getenv("WAKE_READY_INTERVAL")); v != "" {
		parsedInterval, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse WAKE_READY_INTERVAL: %w", err)
		}
		interval = parsedInterval
	}

	checkPath := strings.TrimSpace(os.Getenv("WAKE_READY_PATH"))
	if checkPath == "" {
		checkPath = "/version"
	}

	return &readinessWaitAction{
		client:    client,
		timeout:   timeout,
		interval:  interval,
		checkPath: checkPath,
	}, nil
}

func newArgoClusterRefreshActionFromEnv() (wakeAcceptedAction, error) {
	namespace := strings.TrimSpace(os.Getenv("ARGOCD_CLUSTER_REFRESH_SECRET_NAMESPACE"))
	secretName := strings.TrimSpace(os.Getenv("ARGOCD_CLUSTER_REFRESH_SECRET_NAME"))
	secretNameTemplate := strings.TrimSpace(os.Getenv("ARGOCD_CLUSTER_REFRESH_SECRET_NAME_TEMPLATE"))

	if namespace == "" && secretName == "" && secretNameTemplate == "" {
		return nil, nil
	}
	if namespace == "" {
		return nil, errors.New("ARGOCD_CLUSTER_REFRESH_SECRET_NAMESPACE is required when Argo cluster refresh is enabled")
	}
	if secretName == "" && secretNameTemplate == "" {
		return nil, errors.New("set ARGOCD_CLUSTER_REFRESH_SECRET_NAME or ARGOCD_CLUSTER_REFRESH_SECRET_NAME_TEMPLATE when Argo cluster refresh is enabled")
	}
	if secretName != "" && secretNameTemplate != "" {
		return nil, errors.New("set only one of ARGOCD_CLUSTER_REFRESH_SECRET_NAME or ARGOCD_CLUSTER_REFRESH_SECRET_NAME_TEMPLATE")
	}

	apiBase := strings.TrimSpace(os.Getenv("ARGOCD_CLUSTER_REFRESH_KUBERNETES_API"))
	if apiBase == "" {
		var err error
		apiBase, err = inClusterKubernetesAPIBase()
		if err != nil {
			return nil, err
		}
	}

	tokenPath := mustEnv("ARGOCD_CLUSTER_REFRESH_TOKEN_PATH", defaultKubernetesServiceAccountTokenPath)
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("read Argo cluster refresh token %q: %w", tokenPath, err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return nil, fmt.Errorf("Argo cluster refresh token %q is empty", tokenPath)
	}

	refreshTimeout := 5 * time.Second
	if v := strings.TrimSpace(os.Getenv("ARGOCD_CLUSTER_REFRESH_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse ARGOCD_CLUSTER_REFRESH_TIMEOUT: %w", err)
		}
		refreshTimeout = d
	}

	caPath := mustEnv("ARGOCD_CLUSTER_REFRESH_CA_PATH", defaultKubernetesServiceAccountCAPath)
	client, err := newClusterRefreshHTTPClient(apiBase, caPath, refreshTimeout)
	if err != nil {
		return nil, err
	}

	return &argoClusterSecretRefreshAction{
		client:             client,
		apiBase:            apiBase,
		bearerToken:        token,
		namespace:          namespace,
		secretName:         secretName,
		secretNameTemplate: secretNameTemplate,
	}, nil
}

func newWakeAcceptedActionFromEnv(client *http.Client) (wakeAcceptedAction, error) {
	var actions []wakeAcceptedAction

	readinessAction, err := newReadinessWaitActionFromEnv(client)
	if err != nil {
		return nil, err
	}
	if readinessAction != nil {
		actions = append(actions, readinessAction)
	}

	clusterRefreshAction, err := newArgoClusterRefreshActionFromEnv()
	if err != nil {
		return nil, err
	}
	if clusterRefreshAction != nil {
		actions = append(actions, clusterRefreshAction)
	}

	switch len(actions) {
	case 0:
		return nil, nil
	case 1:
		return actions[0], nil
	default:
		return compositeWakeAcceptedAction{actions: actions}, nil
	}
}

func isRetryableWakeError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return isRetryableWakeError(urlErr.Err)
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return isRetryableWakeError(opErr.Err)
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.ECONNRESET
	}

	return strings.Contains(strings.ToLower(err.Error()), "server closed idle connection")
}

func writeAcceptedWakeResponse(w http.ResponseWriter, note string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":       true,
		"accepted": true,
		"note":     note,
	})
}

func executeWakeAcceptedAction(ctx context.Context, action wakeAcceptedAction, wake wakeRequestInfo) {
	if action == nil {
		return
	}

	if err := action.Execute(ctx, wake); err != nil {
		log.Printf("accepted wake request for project=%s virtualcluster=%s, but post-wake action failed: %v", wake.Project, wake.VirtualCluster, err)
	}
}

func writeAcceptedWake(ctx context.Context, w http.ResponseWriter, action wakeAcceptedAction, wake wakeRequestInfo, note string) {
	executeWakeAcceptedAction(ctx, action, wake)
	writeAcceptedWakeResponse(w, note)
}

func newProxyHandler(upstream string, client *http.Client, successOn map[int]struct{}, successOnError bool, action wakeAcceptedAction) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		targetURL := strings.TrimRight(upstream, "/") + r.URL.Path
		if r.URL.RawQuery != "" {
			targetURL += "?" + r.URL.RawQuery
		}
		wake, wakeRequest := parseWakeRequest(r)
		forwardHeaders := cloneForwardHeaders(r.Header)
		if wakeRequest {
			wake.UpstreamURL = strings.TrimRight(upstream, "/") + r.URL.Path
			wake.ForwardHeaders = forwardHeaders.Clone()
		}

		req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
		if err != nil {
			http.Error(w, "bad upstream request", http.StatusBadGateway)
			return
		}
		req.Header = forwardHeaders.Clone()

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("upstream %s %s error: %v", r.Method, targetURL, err)
			if successOnError && wakeRequest && isRetryableWakeError(err) {
				log.Printf("wake request %s error (%v) treated as accepted", targetURL, err)
				writeAcceptedWake(r.Context(), w, action, wake, "wake request likely initiated; retryable upstream transport error treated as accepted")
				return
			}

			http.Error(w, "upstream error: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		log.Printf("upstream %s %s -> %s", r.Method, targetURL, resp.Status)

		if _, ok := successOn[resp.StatusCode]; ok && wakeRequest {
			log.Printf("wake request %s -> %d (treated as accepted)", targetURL, resp.StatusCode)
			_, _ = io.Copy(io.Discard, resp.Body)
			writeAcceptedWake(r.Context(), w, action, wake, "wake request likely initiated; retryable upstream status treated as accepted")
			return
		}
		if wakeRequest && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted) {
			log.Printf("wake request %s -> %d (rewritten as accepted JSON)", targetURL, resp.StatusCode)
			_, _ = io.Copy(io.Discard, resp.Body)
			writeAcceptedWake(r.Context(), w, action, wake, "wake request accepted")
			return
		}

		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Printf("stream error: %v", err)
		}
	}
}

func main() {
	upstream := os.Getenv("UPSTREAM_BASE")
	if upstream == "" {
		log.Fatal("UPSTREAM_BASE is required")
	}
	successOn := parseOverrideSet(mustEnv("SUCCESS_ON", "502,504"))
	addr := mustEnv("LISTEN_ADDR", ":8080")
	timeout := 10 * time.Second
	if t := os.Getenv("UPSTREAM_TIMEOUT"); t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			timeout = d
		}
	}
	successOnError := mustEnv("SUCCESS_ON_ERROR", "false") == "true"

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       30 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: timeout}

	wakeAcceptedAction, err := newWakeAcceptedActionFromEnv(client)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	http.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	http.HandleFunc("/", newProxyHandler(upstream, client, successOn, successOnError, wakeAcceptedAction))

	dump := mustEnv("LOG_REQUESTS", "false") == "true"
	skippedDumpUserAgents := parseList(mustEnv("LOG_REQUESTS_SKIP_USER_AGENTS", "kube-probe"))
	if dump {
		orig := http.DefaultServeMux
		http.DefaultServeMux = http.NewServeMux()
		http.DefaultServeMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if shouldDumpRequest(r, skippedDumpUserAgents) {
				if b, err := httputil.DumpRequest(r, true); err == nil {
					log.Printf("REQ:\n%s\n", string(b))
				}
			}
			orig.ServeHTTP(w, r)
		})
	}

	log.Printf(
		"proxy listening on %s -> upstream %s (wake success on: %v, wake success on transport error: %v, timeout: %s, request log skip user agents: %v)",
		addr,
		upstream,
		successOn,
		successOnError,
		timeout,
		skippedDumpUserAgents,
	)
	log.Fatal(http.ListenAndServe(addr, nil))
}
