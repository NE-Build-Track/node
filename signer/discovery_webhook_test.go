package signer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrchestratorWebhookDiscovery(t *testing.T) {
	var webhookCalls, runnerCalls, staticCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		switch r.URL.Path {
		case "/list":
			webhookCalls.Add(1)
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "token=private-token", r.URL.RawQuery, "client filters must not alter the webhook URL")
			require.Equal(t, "Bearer configured", r.Header.Get("Authorization"))
			require.Equal(t, []string{"one", "two"}, r.Header.Values("X-Webhook"))
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `[{"address":%q,"score":"ignored"},{"address":"http://%%private-token"},{"address":"file:///bad"}]`, base+"/orch%2Fdeployment/catalog")
		case "/orch/deployment/catalog":
			require.Equal(t, "/orch%2Fdeployment/catalog", r.URL.EscapedPath())
			calls := runnerCalls.Add(1)
			require.Empty(t, r.Header.Get("Authorization"))
			require.Empty(t, r.Header.Get("X-Webhook"))
			require.Empty(t, r.URL.RawQuery, "fetch complete inventories before filtering")
			_, _ = fmt.Fprintf(w, `[{"address":%q,"score":{"ignored":true},"runners":[{"url":%q,"observed_at":"2026-01-01T00:00:00Z","app":"example","gpu":{"name":" H100 "},"capacity_used":%d},{"url":%q,"observed_at":"2026-01-01T00:00:00Z","app":"other"}]}]`, base, base+"/app", calls, base+"/other")
		default:
			staticCalls.Add(1)
		}
	}))
	defer server.Close()
	s := newService(nil)
	defer s.Close()
	require.NoError(t, s.SetDiscovery([]*url.URL{testURL(t, server.URL+"/static")}, "127.0.0.1"))
	endpoint := testURL(t, server.URL+"/list?token=private-token")
	headers := Headers{"Authorization": {"Bearer configured"}, "X-Webhook": {"one", "two"}}
	require.NoError(t, s.SetOrchestratorWebhook(endpoint, headers, 5*time.Minute))
	endpoint.Path = "/mutated"
	headers["Authorization"][0] = "mutated"
	for i := 1; i <= 2; i++ {
		w := discoveryResponse(s, "app=example&app=missing&gpu=H100")
		require.Equal(t, 200, w.Code, w.Body.String())
		require.NotContains(t, w.Body.String(), `"score"`)
		var entries []discoveredOrchestrator
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &entries))
		require.Len(t, entries, 1)
		require.Len(t, entries[0].Runners, 1)
		require.Equal(t, i, entries[0].Runners[0].CapacityUsed, "runner state must remain fresh")
	}
	require.Equal(t, int32(1), webhookCalls.Load())
	require.Equal(t, int32(2), runnerCalls.Load())
	require.Zero(t, staticCalls.Load())
}

type webhookTransport func(*http.Request) (*http.Response, error)

func (f webhookTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOrchestratorWebhookRefresh(t *testing.T) {
	logs := captureSignerLogs(t)
	synctest.Test(t, func(t *testing.T) {
		const interval = 11 * time.Minute
		s := newService(nil)
		defer s.Close()
		endpoint := testURL(t, "https://webhook.example?private-token=secret")
		require.Error(t, s.SetOrchestratorWebhook(endpoint, nil, interval))
		require.NoError(t, s.SetDiscovery(nil))
		require.NoError(t, s.SetOrchestratorWebhook(endpoint, nil, interval))
		var calls, cancellations atomic.Int32
		var stall atomic.Bool
		var body atomic.Value
		stall.Store(true)
		body.Store("private-detail")
		s.discoveryClient.Transport = webhookTransport(func(r *http.Request) (*http.Response, error) {
			data := "[]"
			if r.URL.Host == endpoint.Host {
				calls.Add(1)
				if stall.Load() {
					<-r.Context().Done()
					cancellations.Add(1)
					return nil, r.Context().Err()
				}
				data = body.Load().(string)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(data))}, nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); s.RunOrchestratorWebhook(ctx) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		var requests sync.WaitGroup
		for range 8 {
			requests.Go(func() { require.Equal(t, 503, discoveryResponse(s, "").Code) })
		}
		requestCtx, cancelRequest := context.WithCancel(t.Context())
		defer cancelRequest()
		canceled := httptest.NewRecorder()
		requests.Go(func() {
			s.ServeHTTP(canceled, httptest.NewRequest(http.MethodGet, "/discover-orchestrators", nil).WithContext(requestCtx))
		})
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load(), "startup requests share the worker's fetch")
		cancelRequest()
		synctest.Wait()
		require.Equal(t, 503, canceled.Code, "a canceled waiter returns before the shared fetch completes")
		require.Zero(t, cancellations.Load(), "canceling a waiter must not cancel the shared fetch")
		time.Sleep(3 * time.Second)
		synctest.Wait()
		requests.Wait()
		require.Equal(t, int32(1), calls.Load(), "one shared timeout must release every request")
		stall.Store(false)
		require.Equal(t, 503, discoveryResponse(s, "").Code)
		body.Store(`[{"address":"https://orch.example/catalog","score":42}]`)
		require.Equal(t, 200, discoveryResponse(s, "").Code, "retry lazily before the next interval")
		body.Store("private-detail")
		time.Sleep(interval - 4*time.Second)
		synctest.Wait()
		require.Equal(t, int32(3), calls.Load(), "respect the custom interval")
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 200, discoveryResponse(s, "").Code)
		require.Equal(t, int32(4), calls.Load(), "cached discovery must not retry refreshes")
		body.Store("[]")
		time.Sleep(interval)
		synctest.Wait()
		require.Equal(t, 503, discoveryResponse(s, "").Code, "empty success clears the list")
		require.Equal(t, int32(5), calls.Load(), "empty success ends lazy retries too")
		body.Store(`[{"address":"https://recovered.example/custom","score":{"ignored":true}}]`)
		time.Sleep(interval)
		synctest.Wait()
		require.Equal(t, "https://recovered.example/custom", s.discoveryURLs(t.Context())[0].String())
		stall.Store(true)
		time.Sleep(interval)
		synctest.Wait()
		require.Equal(t, 200, discoveryResponse(s, "").Code, "cached discovery must not wait for refresh")
		time.Sleep(3 * time.Second)
		synctest.Wait()
		require.Equal(t, int32(2), cancellations.Load(), "refreshes time out after three seconds")
		require.Equal(t, 200, discoveryResponse(s, "").Code, "failed refresh retains cached discovery")
		time.Sleep(interval - 3*time.Second)
		synctest.Wait()
		cancel()
		synctest.Wait()
		require.Equal(t, int32(3), cancellations.Load(), "shutdown cancels the in-flight refresh")
	})
	require.NotContains(t, logs.String(), "private")
}

func TestOrchestratorWebhookFailures(t *testing.T) {
	for name, test := range map[string]struct {
		status int
		body   string
	}{
		"status": {500, "private-detail"}, "redirect": {302, ""},
		"malformed": {200, "private-detail"}, "trailing": {200, "[] {}"}, "null": {200, "null"},
		"oversized": {200, "[]" + strings.Repeat(" ", 1<<20)}, "read error": {200, "[]"},
		"address type": {200, `[{"address":"https://good.example/catalog"},{"address":123}]`},
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			transport := webhookTransport(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				var body io.Reader = strings.NewReader(test.body)
				if name == "read error" {
					body = iotest.ErrReader(io.ErrUnexpectedEOF)
				}
				return &http.Response{StatusCode: test.status, Header: http.Header{"Location": {"/private-target"}}, Body: io.NopCloser(body)}, nil
			})
			h := orchestratorWebhook{endpoint: testURL(t, "https://private-webhook.example?private-token=secret")}
			_, err := h.fetch(t.Context(), transport)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private")
			require.Equal(t, int32(1), calls.Load(), "never follow redirects")
		})
	}
}

func TestOrchestratorWebhookGrants(t *testing.T) {
	var runnerCalls, webhookCalls atomic.Int32
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runnerCalls.Add(1)
		_, _ = io.WriteString(w, `[]`)
	}))
	defer runner.Close()
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookCalls.Add(1)
		_, _ = fmt.Fprintf(w, `[{"address":%q}]`, runner.URL)
	}))
	defer webhook.Close()
	s := newService(nil)
	defer s.Close()
	require.NoError(t, s.SetDiscovery(nil, "127.0.0.1"))
	require.NoError(t, s.SetOrchestratorWebhook(testURL(t, webhook.URL), nil, time.Minute))
	for _, test := range []struct {
		name        string
		grants      []string
		status      int
		hooks, runs int32
	}{
		{"webhook denied", nil, 503, 0, 0},
		{"both granted", []string{testURL(t, webhook.URL).Host, testURL(t, runner.URL).Host}, 200, 1, 1},
		{"runner denied", []string{testURL(t, webhook.URL).Host}, 503, 1, 0},
		{"webhook revoked", nil, 503, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			webhookCalls.Store(0)
			runnerCalls.Store(0)
			require.NoError(t, s.SetDiscovery(nil, test.grants...))
			s.orchWebhook.refresh(t.Context(), s.discoveryClient.Transport, false)
			require.Equal(t, test.status, discoveryResponse(s, "").Code)
			require.Equal(t, test.hooks, webhookCalls.Load())
			require.Equal(t, test.runs, runnerCalls.Load())
		})
	}
}
