package signer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/discovery"
	"github.com/stretchr/testify/require"
)

func discoveryResponse(s *Service, query string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/discover-orchestrators?"+query, nil))
	return w
}

func TestSignerDiscovery(t *testing.T) {
	const advertised = `[{"address":"http://localhost:9000","runners":[{"url":"http://localhost:9000/app","observed_at":"2026-01-01T00:00:00Z","app":"retained","version":"v1","metadata":"model-x","mode":"single-shot","gpu":{"id":"0","name":" H100 ","vram_mb":80000},"capacity":3,"capacity_used":1,"capacity_available":2,"price_info":{"price":10,"price_usd":0.1,"currency":"wei","unit":"fixed"}},{"url":"http://localhost:9000/cpu","observed_at":"2026-01-01T00:00:00Z","app":"cpu","mode":"single-shot","capacity":1,"price_info":{"price":1,"currency":"wei","unit":"fixed"}}]}]`
	bodies := map[string]string{
		"empty": "[]", "good": advertised, "failed": "", "malformed": "not JSON",
		"metadata":  `[{"nodes":[]}]`,
		"truncated": `[{"address":`, "oversized": `[{"address":"` + strings.Repeat("a", 1<<20) + `"}]`,
		"invalid": `[{"address":"file:///invalid","runners":[{"url":"http://localhost/app","app":"example"}]},{"address":"https://orch.example","runners":[{"url":"http://localhost/app"},{"app":"example"}]}]`,
	}
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.URL.RawQuery, "fetch complete inventories before filtering")
		source := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), "/discovery")
		if source == "failed" {
			w.WriteHeader(500)
			return
		}
		_, _ = io.WriteString(w, bodies[source])
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	s := newService(nil)
	defer s.Close()
	sources := map[string]*url.URL{"denied": testURL(t, "http://10.1.2.3:8935")}
	for source := range bodies {
		sources[source] = testURL(t, server.URL+"/"+source)
	}
	endpoint := sources["empty"]
	for _, grants := range [][]string{nil, {net.JoinHostPort("127.0.0.2", endpoint.Port())}, {net.JoinHostPort(endpoint.Hostname(), "1")}} {
		require.NoError(t, s.SetDiscovery([]*url.URL{endpoint}, grants...))
		require.Equal(t, 503, discoveryResponse(s, "").Code, grants)
		require.Zero(t, connections.Load(), "denial must happen before connecting")
	}
	collector, err := discovery.NewServer(discovery.Params{
		Listen: netip.MustParseAddrPort("127.0.0.1:8939"), MetricsListen: netip.MustParseAddrPort("127.0.0.1:8940"),
		Nodes: []boa.Text[*url.URL]{{Value: sources["good"]}}, Grants: []string{endpoint.Hostname()},
		AdvertiseNodes: true, AdvertiseRunners: true, RefreshInterval: time.Minute, MaxFailedRefreshes: 6,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); collector.Run(ctx) }()
	defer func() { cancel(); <-done; collector.Close() }()
	cache := httptest.NewServer(collector)
	defer cache.Close()
	sources["collected"] = testURL(t, cache.URL)
	require.Eventually(t, func() bool {
		w := httptest.NewRecorder()
		collector.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/discovery", nil))
		var entries []json.RawMessage
		return json.Unmarshal(w.Body.Bytes(), &entries) == nil && len(entries) == 2
	}, 3*time.Second, 10*time.Millisecond)
	var expected []discoveredOrchestrator
	require.NoError(t, json.Unmarshal([]byte(advertised), &expected))
	for _, test := range []struct {
		sources []string
		query   string
		status  int
		apps    []string
	}{
		{nil, "", 503, nil},
		{[]string{"empty"}, "", 200, nil}, {[]string{"invalid"}, "", 503, nil},
		{[]string{"metadata"}, "", 200, nil},
		{[]string{"failed"}, "", 503, nil}, {[]string{"malformed"}, "", 503, nil},
		{[]string{"truncated"}, "", 503, nil}, {[]string{"oversized"}, "", 503, nil},
		{[]string{"denied", "failed", "malformed", "good"}, "", 200, []string{"retained", "cpu"}},
		{[]string{"collected"}, "", 200, []string{"retained", "cpu"}},
		{[]string{"good"}, "app=missing&app=retained&gpu=L40S&gpu=H100", 200, []string{"retained"}},
		{[]string{"good"}, "app=cpu", 200, []string{"cpu"}},
		{[]string{"good"}, "app=retained&gpu=L40S", 200, nil},
		{[]string{"good"}, "app=cpu&gpu=H100", 200, nil},
	} {
		t.Run(strings.Join(test.sources, ",")+"?"+test.query, func(t *testing.T) {
			urls := make([]*url.URL, len(test.sources))
			for i, source := range test.sources {
				urls[i] = sources[source]
			}
			require.NoError(t, s.SetDiscovery(urls, endpoint.Hostname()))
			w := discoveryResponse(s, test.query)
			require.Equal(t, test.status, w.Code, w.Body.String())
			if test.status != 200 {
				return
			}
			if len(test.apps) == 0 {
				require.JSONEq(t, "[]", w.Body.String())
				return
			}
			var result []discoveredOrchestrator
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
			require.Len(t, result, 1)
			require.Equal(t, expected[0].Address, result[0].Address)
			want := slices.DeleteFunc(slices.Clone(expected[0].Runners), func(r discoveredRunner) bool { return !slices.Contains(test.apps, r.App) })
			require.Equal(t, want, result[0].Runners, "preserve advertised fields and URLs")
		})
	}
}

type discoveryCloseTracker struct {
	http.Transport
	closed bool
}

func (t *discoveryCloseTracker) CloseIdleConnections() { t.closed = true }

func TestSignerDiscoveryDeploymentPaths(t *testing.T) {
	paths := []string{"", "/deployment", "/deploy%2F", "/a%2Fb", "/%7Btenant%7D"}
	mux := http.NewServeMux()
	for _, path := range paths {
		mux.HandleFunc("GET "+path+"/discovery", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "[]")
		})
	}
	upstream := httptest.NewServer(mux)
	defer upstream.Close()
	s := newService(nil)
	defer s.Close()
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			require.NoError(t, s.SetDiscovery([]*url.URL{testURL(t, upstream.URL+path+"/")}, "127.0.0.1"))
			w := discoveryResponse(s, "")
			require.Equal(t, http.StatusOK, w.Code)
			require.JSONEq(t, "[]", w.Body.String())
		})
	}
}

func TestSignerDiscoveryReplacement(t *testing.T) {
	s := newService(nil)
	defer s.Close()
	require.Equal(t, 503, discoveryResponse(s, "").Code)
	endpoint := testURL(t, "https://orch.example/deployment/")
	require.NoError(t, s.SetDiscovery([]*url.URL{endpoint}))
	require.Equal(t, "/deployment/", endpoint.Path)
	require.Equal(t, 5*time.Second, s.collector.Client.Timeout)
	previous := s.collector.Client
	previousCollector := s.collector
	tracker := new(discoveryCloseTracker)
	previous.Transport = tracker
	for _, invalid := range []*url.URL{nil, testURL(t, "file:///discovery"), testURL(t, "https://user:password@orch.example"), testURL(t, "https://orch.example?token=secret"), testURL(t, "https://orch.example?"), testURL(t, "https://orch.example#fragment")} {
		require.Error(t, s.SetDiscovery([]*url.URL{invalid}))
		require.Same(t, previous, s.collector.Client)
	}
	require.Error(t, s.SetDiscovery([]*url.URL{endpoint}, "ftp://localhost"))
	require.Same(t, previous, s.collector.Client)
	require.Same(t, previousCollector, s.collector)
	require.False(t, tracker.closed)
	require.NoError(t, s.SetDiscovery(nil))
	require.Equal(t, 503, discoveryResponse(s, "").Code)
	require.True(t, tracker.closed)
}

func TestSignerDiscoveryFreshnessMerge(t *testing.T) {
	s := newService(nil)
	defer s.Close()
	require.NoError(t, s.SetDiscovery([]*url.URL{testURL(t, "https://first.example"), testURL(t, "https://second.example")}))
	const old = `[{"address":"https://owner-old.example","runners":[{"url":"https://runner.example/app","app":"old","gpu":{"name":"H100"},"observed_at":"2026-01-01T00:00:00Z","capacity":99}]}]`
	const fresh = `[{"address":"https://owner-new.example","runners":[{"url":"https://runner.example/app","app":"new","gpu":{"name":"L40S"},"observed_at":"2026-01-01T02:00:00.123+01:00","capacity":1},{"url":"https://other.example/app","app":"new","gpu":{"name":"L40S"},"observed_at":"2026-01-01T00:00:00Z"}]}]`
	var offline atomic.Bool
	s.collector.Client.Transport = webhookTransport(func(r *http.Request) (*http.Response, error) {
		body := old
		if r.URL.Host == "second.example" {
			if offline.Load() {
				return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			body = fresh
		}
		// Model a source that honors filters: forwarding them would hide the newer record.
		var entries []discoveredOrchestrator
		require.NoError(t, json.Unmarshal([]byte(body), &entries))
		app, gpu := r.URL.Query().Get("app"), r.URL.Query().Get("gpu")
		entries[0].Runners = slices.DeleteFunc(entries[0].Runners, func(runner discoveredRunner) bool {
			return app != "" && runner.App != app || gpu != "" && runner.GPU.Name != gpu
		})
		data, err := json.Marshal(entries)
		require.NoError(t, err)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(data)))}, nil
	})
	w := discoveryResponse(s, "")
	require.Equal(t, 200, w.Code)
	var result []discoveredOrchestrator
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Len(t, result, 1)
	require.Equal(t, "https://owner-new.example", result[0].Address)
	require.Len(t, result[0].Runners, 2)
	require.Equal(t, 1, result[0].Runners[0].Capacity)
	require.Equal(t, "2026-01-01T02:00:00.123+01:00", result[0].Runners[0].ObservedAt)
	for _, query := range []string{"app=old", "gpu=H100", "app=old&gpu=H100"} {
		require.JSONEq(t, "[]", discoveryResponse(s, query).Body.String(), "resolve duplicates before filtering")
	}
	offline.Store(true)
	for range 6 {
		s.collector.Refresh(t.Context(), false)
	}
	w = discoveryResponse(s, "")
	require.Equal(t, 200, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Len(t, result, 1)
	require.Equal(t, "https://owner-old.example", result[0].Address)
}
