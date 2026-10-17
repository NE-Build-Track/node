package signer

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func discoveryResponse(s *Service, query string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/discover-orchestrators?"+query, nil))
	return w
}

func TestSignerDiscovery(t *testing.T) {
	const advertised = `[{"address":"http://localhost:9000","runners":[{"url":"http://localhost:9000/app","app":"retained","version":"v1","metadata":"model-x","mode":"single-shot","gpu":{"id":"0","name":" H100 ","vram_mb":80000},"capacity":3,"capacity_used":1,"capacity_available":2,"price_info":{"price":10,"price_usd":0.1,"currency":"wei","unit":"fixed"}},{"url":"http://localhost:9000/cpu","app":"cpu","mode":"single-shot","capacity":1,"price_info":{"price":1,"currency":"wei","unit":"fixed"}}]}]`
	bodies := map[string]string{
		"empty": "[]", "good": advertised, "failed": "", "malformed": "not JSON",
		"truncated": `[{"address":`, "oversized": `[{"address":"` + strings.Repeat("a", 1<<20) + `"}]`,
		"invalid": `[{"address":"file:///invalid","runners":[{"url":"http://localhost/app","app":"example"}]},{"address":"https://orch.example","runners":[{"url":"http://localhost/app"},{"app":"example"}]}]`,
	}
	queries := make(chan url.Values, 1)
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		source := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), "/discovery")
		if source == "failed" {
			w.WriteHeader(500)
			return
		}
		if source == "good" {
			queries <- r.URL.Query()
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
	var expected []discoveredOrchestrator
	require.NoError(t, json.Unmarshal([]byte(advertised), &expected))
	for _, test := range []struct {
		sources []string
		query   string
		status  int
		apps    []string
	}{
		{nil, "", 503, nil},
		{[]string{"empty"}, "", 200, nil}, {[]string{"invalid"}, "", 200, nil},
		{[]string{"failed"}, "", 503, nil}, {[]string{"malformed"}, "", 503, nil},
		{[]string{"truncated"}, "", 503, nil}, {[]string{"oversized"}, "", 503, nil},
		{[]string{"denied", "failed", "malformed", "good"}, "", 200, []string{"retained", "cpu"}},
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
			if slices.Contains(test.sources, "good") {
				query, err := url.ParseQuery(test.query)
				require.NoError(t, err)
				require.Equal(t, query, <-queries, "forward all app and GPU filters")
			}
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

func TestSignerDiscoveryReplacement(t *testing.T) {
	s := newService(nil)
	defer s.Close()
	require.Equal(t, 503, discoveryResponse(s, "").Code)
	endpoint := testURL(t, "https://orch.example/deployment/")
	require.NoError(t, s.SetDiscovery([]*url.URL{endpoint}))
	require.Equal(t, "https://orch.example/deployment/discovery", s.discoveryURLs(t.Context())[0].String())
	require.Equal(t, "/deployment/", endpoint.Path)
	require.Equal(t, 5*time.Second, s.discoveryClient.Timeout)
	previous := s.discoveryClient
	tracker := new(discoveryCloseTracker)
	previous.Transport = tracker
	for _, invalid := range []*url.URL{nil, testURL(t, "file:///discovery"), testURL(t, "https://user:password@orch.example"), testURL(t, "https://orch.example?token=secret"), testURL(t, "https://orch.example#fragment")} {
		require.Error(t, s.SetDiscovery([]*url.URL{invalid}))
		require.Same(t, previous, s.discoveryClient)
	}
	require.Error(t, s.SetDiscovery([]*url.URL{endpoint}, "ftp://localhost"))
	require.Same(t, previous, s.discoveryClient)
	require.Equal(t, "https://orch.example/deployment/discovery", s.discoveryURLs(t.Context())[0].String())
	require.False(t, tracker.closed)
	require.NoError(t, s.SetDiscovery(nil))
	require.Empty(t, s.discoveryURLs(t.Context()))
	require.True(t, tracker.closed)
}
