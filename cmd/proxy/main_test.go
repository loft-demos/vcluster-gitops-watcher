package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type recordingWakeAction struct {
	calls []wakeRequestInfo
	err   error
}

func (a *recordingWakeAction) Execute(_ context.Context, wake wakeRequestInfo) error {
	a.calls = append(a.calls, wake)
	return a.err
}

func newTestClient(fn roundTripFunc) *http.Client {
	return &http.Client{Transport: fn}
}

func TestShouldDumpRequestSkipsConfiguredUserAgents(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	req.Header.Set("User-Agent", "kube-probe/1.33")

	if shouldDumpRequest(req, parseList("kube-probe")) {
		t.Fatal("expected kube-probe request to be skipped")
	}
}

func TestShouldDumpRequestAllowsOtherUserAgents(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	req.Header.Set("User-Agent", "curl/8.7.1")

	if !shouldDumpRequest(req, parseList("kube-probe")) {
		t.Fatal("expected non-probe request to be logged")
	}
}

func TestParseListTrimsAndNormalizesEntries(t *testing.T) {
	got := parseList(" kube-probe , Prometheus ")
	want := []string{"kube-probe", "prometheus"}

	if len(got) != len(want) {
		t.Fatalf("expected %d items, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestWakeRequestTreatsConfiguredStatusAsAcceptedAndRunsAction(t *testing.T) {
	action := &recordingWakeAction{}
	handler := newProxyHandler("http://upstream", newTestClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusGatewayTimeout,
			Status:     "504 Gateway Timeout",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("timeout")),
		}, nil
	}), parseOverrideSet("502,504"), false, action)

	req := httptest.NewRequest(http.MethodPost, "/kubernetes/project/demo/virtualcluster/team-a", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"accepted":true`) {
		t.Fatalf("expected accepted response body, got %q", rec.Body.String())
	}
	if len(action.calls) != 1 {
		t.Fatalf("expected one wake action call, got %d", len(action.calls))
	}
	if action.calls[0].Project != "demo" || action.calls[0].VirtualCluster != "team-a" {
		t.Fatalf("unexpected wake action call: %#v", action.calls[0])
	}
}

func TestWakeRequestRewritesSuccessfulResponseAsAcceptedJSON(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		status     string
	}{
		{name: "200 OK", statusCode: http.StatusOK, status: "200 OK"},
		{name: "202 Accepted", statusCode: http.StatusAccepted, status: "202 Accepted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newProxyHandler("http://upstream", newTestClient(func(*http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Content-Type", "application/json")
				header.Set("X-Upstream", "set")
				return &http.Response{
					StatusCode: tt.statusCode,
					Status:     tt.status,
					Header:     header,
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			}), parseOverrideSet("502,504"), false, nil)

			req := httptest.NewRequest(http.MethodPost, "/kubernetes/project/demo/virtualcluster/team-a", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("expected synthetic 200, got %d", rec.Code)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("expected synthetic JSON content type, got %q", got)
			}
			if got := rec.Header().Get("X-Upstream"); got != "" {
				t.Fatalf("expected upstream headers to be discarded, got %q", got)
			}
			if !strings.Contains(rec.Body.String(), `"accepted":true`) {
				t.Fatalf("expected accepted response body, got %q", rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"note":"wake request accepted"`) {
				t.Fatalf("expected wake success note, got %q", rec.Body.String())
			}
		})
	}
}

func TestNonWakeRequestPassesConfiguredStatusThroughAndSkipsAction(t *testing.T) {
	action := &recordingWakeAction{}
	handler := newProxyHandler("http://upstream", newTestClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusGatewayTimeout,
			Status:     "504 Gateway Timeout",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("timeout")),
		}, nil
	}), parseOverrideSet("502,504"), false, action)

	req := httptest.NewRequest(http.MethodGet, "/kubernetes/project/demo/virtualcluster/team-a", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504, got %d", rec.Code)
	}
	if rec.Body.String() != "timeout" {
		t.Fatalf("expected upstream body to pass through, got %q", rec.Body.String())
	}
	if len(action.calls) != 0 {
		t.Fatalf("expected no wake action calls, got %d", len(action.calls))
	}
}

func TestNonWakeRequestPassesSuccessfulResponseThrough(t *testing.T) {
	handler := newProxyHandler("http://upstream", newTestClient(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Content-Type", "application/json")
		header.Set("X-Upstream", "set")
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Status:     "202 Accepted",
			Header:     header,
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	}), parseOverrideSet("502,504"), false, nil)

	req := httptest.NewRequest(http.MethodGet, "/kubernetes/project/demo/virtualcluster/team-a", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-Upstream"); got != "set" {
		t.Fatalf("expected upstream headers to pass through, got %q", got)
	}
	if rec.Body.String() != "" {
		t.Fatalf("expected empty upstream body to pass through, got %q", rec.Body.String())
	}
}

func TestWakeRequestKeepsPermanentErrorsVisibleAndSkipsAction(t *testing.T) {
	action := &recordingWakeAction{}
	handler := newProxyHandler("http://upstream", newTestClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Status:     "403 Forbidden",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("forbidden")),
		}, nil
	}), parseOverrideSet("502,504"), true, action)

	req := httptest.NewRequest(http.MethodPost, "/kubernetes/project/demo/virtualcluster/team-a", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
	if rec.Body.String() != "forbidden" {
		t.Fatalf("expected upstream body to pass through, got %q", rec.Body.String())
	}
	if len(action.calls) != 0 {
		t.Fatalf("expected no wake action calls, got %d", len(action.calls))
	}
}

func TestWakeRequestTreatsRetryableTransportErrorAsAccepted(t *testing.T) {
	handler := newProxyHandler("http://upstream", newTestClient(func(*http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Post", URL: "http://upstream", Err: io.ErrUnexpectedEOF}
	}), parseOverrideSet("502,504"), true, nil)

	req := httptest.NewRequest(http.MethodPost, "/kubernetes/project/demo/virtualcluster/team-a", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"accepted":true`) {
		t.Fatalf("expected accepted response body, got %q", rec.Body.String())
	}
}

func TestWakeRequestActionFailureStillReturnsAccepted(t *testing.T) {
	action := &recordingWakeAction{err: errors.New("boom")}
	handler := newProxyHandler("http://upstream", newTestClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Status:     "202 Accepted",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	}), parseOverrideSet("502,504"), false, action)

	req := httptest.NewRequest(http.MethodPost, "/kubernetes/project/demo/virtualcluster/team-a", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(action.calls) != 1 {
		t.Fatalf("expected one wake action call, got %d", len(action.calls))
	}
}

func TestWakeRequestDoesNotHideNonRetryableTransportError(t *testing.T) {
	handler := newProxyHandler("http://upstream", newTestClient(func(*http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Post", URL: "http://upstream", Err: errors.New("no such host")}
	}), parseOverrideSet("502,504"), true, nil)

	req := httptest.NewRequest(http.MethodPost, "/kubernetes/project/demo/virtualcluster/team-a", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no such host") {
		t.Fatalf("expected transport error to be visible, got %q", rec.Body.String())
	}
}

func TestReadinessWaitActionPollsUntilReady(t *testing.T) {
	attempts := 0
	var gotAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		gotAuth = r.Header.Get("Authorization")

		if r.URL.Path != "/kubernetes/project/demos/virtualcluster/jf-demo/version" {
			t.Fatalf("unexpected readiness path: %q", r.URL.Path)
		}

		if attempts == 1 {
			http.Error(w, "waking", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"major":"1","minor":"31"}`))
	}))
	defer server.Close()

	action := &readinessWaitAction{
		client:    server.Client(),
		timeout:   500 * time.Millisecond,
		interval:  10 * time.Millisecond,
		checkPath: "/version",
	}

	err := action.Execute(context.Background(), wakeRequestInfo{
		Project:        "demos",
		VirtualCluster: "jf-demo",
		UpstreamURL:    server.URL + "/kubernetes/project/demos/virtualcluster/jf-demo",
		ForwardHeaders: http.Header{"Authorization": {"Bearer test-token"}},
	})
	if err != nil {
		t.Fatalf("expected readiness wait to succeed, got %v", err)
	}
	if attempts < 2 {
		t.Fatalf("expected readiness probe to retry, got %d attempt(s)", attempts)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("expected readiness probe to forward auth header, got %q", gotAuth)
	}
}

func TestReadinessWaitActionReturnsErrorOnNonRetryableStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer server.Close()

	action := &readinessWaitAction{
		client:    server.Client(),
		timeout:   200 * time.Millisecond,
		interval:  10 * time.Millisecond,
		checkPath: "/version",
	}

	err := action.Execute(context.Background(), wakeRequestInfo{
		Project:        "demos",
		VirtualCluster: "jf-demo",
		UpstreamURL:    server.URL + "/kubernetes/project/demos/virtualcluster/jf-demo",
	})
	if err == nil {
		t.Fatal("expected readiness wait to fail on non-retryable status")
	}
	if !strings.Contains(err.Error(), "403 Forbidden") {
		t.Fatalf("expected 403 in readiness error, got %v", err)
	}
}

func TestArgoClusterSecretRefreshActionPatchesExpectedSecret(t *testing.T) {
	var gotMethod string
	var gotPath string
	var gotAuth string
	var gotContentType string
	var gotPatch map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")

		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&gotPatch); err != nil {
			t.Fatalf("decode patch body: %v", err)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	action := &argoClusterSecretRefreshAction{
		client:             server.Client(),
		apiBase:            server.URL,
		bearerToken:        "test-token",
		namespace:          "argocd",
		secretNameTemplate: "loft-{project}-vcluster-{virtualcluster}",
	}

	err := action.Execute(context.Background(), wakeRequestInfo{Project: "demos", VirtualCluster: "jf-demo"})
	if err != nil {
		t.Fatalf("expected refresh patch to succeed, got %v", err)
	}

	if gotMethod != http.MethodPatch {
		t.Fatalf("expected PATCH, got %q", gotMethod)
	}
	if gotPath != "/api/v1/namespaces/argocd/secrets/loft-demos-vcluster-jf-demo" {
		t.Fatalf("unexpected patch path: %q", gotPath)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if gotContentType != "application/merge-patch+json" {
		t.Fatalf("unexpected content type: %q", gotContentType)
	}

	metadata, ok := gotPatch["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object, got %#v", gotPatch["metadata"])
	}
	annotations, ok := metadata["annotations"].(map[string]any)
	if !ok {
		t.Fatalf("expected annotations object, got %#v", metadata["annotations"])
	}
	rawTimestamp, ok := annotations[argocdClusterRefreshAnnotation].(string)
	if !ok || rawTimestamp == "" {
		t.Fatalf("expected refresh annotation timestamp, got %#v", annotations[argocdClusterRefreshAnnotation])
	}
	if _, err := time.Parse(time.RFC3339Nano, rawTimestamp); err != nil {
		t.Fatalf("expected RFC3339 timestamp, got %q (%v)", rawTimestamp, err)
	}
}

func TestParseWakeRequestAcceptsWatcherVersionProbe(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   bool
	}{
		{http.MethodPost, "/kubernetes/project/demo/virtualcluster/team-a", true},
		{http.MethodPost, "/kubernetes/project/demo/virtualcluster/team-a/api/v1/namespaces", true},
		{http.MethodGet, "/kubernetes/project/demo/virtualcluster/team-a/version", true},
		{http.MethodGet, "/kubernetes/project/demo/virtualcluster/team-a", false},
		{http.MethodGet, "/kubernetes/project/demo/virtualcluster/team-a/api/v1/pods", false},
		{http.MethodGet, "/kubernetes/project/demo/virtualcluster/team-a/version/extra", false},
		{http.MethodGet, "/api/v1/version", false},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, nil)
		info, ok := parseWakeRequest(req)
		if ok != test.want {
			t.Fatalf("%s %s: got %v, want %v", test.method, test.path, ok, test.want)
		}
		if ok && (info.Project != "demo" || info.VirtualCluster != "team-a") {
			t.Fatalf("%s %s: unexpected info %+v", test.method, test.path, info)
		}
	}
}
