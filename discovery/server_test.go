package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/j0sh/boa/pkg/boa"
	"github.com/stretchr/testify/require"
)

func discoveryTestParams(t *testing.T, targets ...string) Params {
	t.Helper()
	p := Params{Listen: netip.MustParseAddrPort("127.0.0.1:8939"), MetricsListen: netip.MustParseAddrPort("127.0.0.1:8940"), AdvertiseNodes: true, AdvertiseRunners: true, RefreshInterval: 5 * time.Minute, MaxFailedRefreshes: 6, Grants: []string{"127.0.0.1"}}
	for _, target := range targets {
		p.Nodes = append(p.Nodes, boa.Text[*url.URL]{Value: mustURL(t, target)})
	}
	return p
}

func discoveryTestServer(t *testing.T, targets ...string) *Server {
	t.Helper()
	s, err := NewServer(discoveryTestParams(t, targets...), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

const childInventory = `[{"address":"https://child.example","nodes":[{"url":"http://127.0.0.1:1"}],"runners":[{"url":"https://child.example/app","app":"test","mode":"single-shot","observed_at":"1999-01-01T00:00:00.123+00:00"}, {"url":"bad"}]},{"address":"https://transitive.example","runners":[{"url":"https://transitive.example/app","app":"test","observed_at":"1998-12-31T00:00:00Z"}]}]`

func TestDiscoveryCacheLifecycle(t *testing.T) {
	var body atomic.Value
	var status, calls, unconfiguredCalls atomic.Int32
	status.Store(200)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.URL.RawQuery)
		switch r.URL.Path {
		case "/fallback/discovery":
			io.WriteString(w, `[{"address":"https://fallback.example","runners":[{"url":"https://child.example/app","app":"test","observed_at":"1998-01-01T00:00:00Z"}]}]`)
		case "/deployment/discovery":
			calls.Add(1)
			w.Header().Set("Location", "/unconfigured/discovery")
			w.WriteHeader(int(status.Load()))
			io.WriteString(w, body.Load().(string))
		default:
			unconfiguredCalls.Add(1)
			io.WriteString(w, childInventory)
		}
	}))
	defer upstream.Close()
	inventory := strings.ReplaceAll(childInventory, "http://127.0.0.1:1", upstream.URL+"/unconfigured")
	body.Store(inventory)
	s := discoveryTestServer(t, upstream.URL+"/deployment", upstream.URL+"/fallback")
	require.Empty(t, s.discoveryResponse())
	s.refresh(t.Context())
	original := s.snapshots[0]
	require.Len(t, original.entries, 2)
	require.Len(t, s.discoveryResponse(), 3)
	require.Equal(t, "1999-01-01T00:00:00.123+00:00", s.discoveryResponse()[1]["runners"].([]discoveryRunner)[0].ObservedAt)
	for range 3 {
		s.discoveryResponse()
	}
	require.Equal(t, int32(1), calls.Load())
	failures := []struct {
		status int
		body   string
	}{{500, inventory}, {200, "bad JSON"}, {200, `[{"address":"https://child.example","runners":[{}]}]`}, {200, strings.Repeat(" ", 1<<20+1)}, {302, inventory}}
	for i, failure := range failures {
		status.Store(int32(failure.status))
		body.Store(failure.body)
		s.refresh(t.Context())
		require.Equal(t, original.node, s.snapshots[0].node)
		require.Equal(t, original.entries, s.snapshots[0].entries)
		require.Equal(t, i+1, s.snapshots[0].failures)
		require.Zero(t, s.snapshots[1].failures)
	}
	require.Equal(t, "https://child.example", s.discoveryResponse()[1]["address"])
	s.refresh(t.Context())
	require.Empty(t, s.snapshots[0].entries)
	require.Len(t, s.discoveryResponse()[0]["nodes"], 1)
	require.Equal(t, "https://fallback.example", s.discoveryResponse()[1]["address"])

	// Recover, then clear a populated cache with an empty success after a failure.
	status.Store(200)
	body.Store(inventory)
	s.refresh(t.Context())
	original = s.snapshots[0]
	require.Len(t, original.entries, 2)
	require.Len(t, s.discoveryResponse()[0]["nodes"], 2)
	require.Zero(t, original.failures)
	status.Store(500)
	s.refresh(t.Context())
	require.Equal(t, 1, s.snapshots[0].failures)
	status.Store(200)
	body.Store("[]")
	s.refresh(t.Context())
	require.Empty(t, s.snapshots[0].entries)
	require.Zero(t, s.snapshots[0].failures)
	require.True(t, s.snapshots[0].node.QueriedAt.After(original.node.QueriedAt))
	require.Len(t, s.discoveryResponse()[0]["nodes"], 2)
	require.Equal(t, "https://fallback.example", s.discoveryResponse()[1]["address"])
	require.Zero(t, unconfiguredCalls.Load(), "neither redirects nor advertised nodes may be followed")
}

type discoveryRoundTrip func(*http.Request) (*http.Response, error)

func (f discoveryRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDiscoveryWorkerSchedulingAndCancellation(t *testing.T) {
	for _, flags := range []struct{ nodes, runners bool }{{true, true}, {true, false}, {false, true}, {false, false}} {
		t.Run(fmt.Sprintf("nodes=%t/runners=%t", flags.nodes, flags.runners), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := discoveryTestServer(t, "https://child.example")
				s.advertiseNodes, s.advertiseRunners = flags.nodes, flags.runners
				require.Empty(t, s.discoveryResponse())
				snapshot := func() nodeSnapshot {
					s.mu.RLock()
					defer s.mu.RUnlock()
					return s.snapshots[0]
				}
				var calls atomic.Int32
				var stall atomic.Bool
				s.client.Transport = discoveryRoundTrip(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					if stall.Load() {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(childInventory))}, nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan struct{})
				go func() { defer close(done); s.Run(ctx) }()
				synctest.Wait()
				result := s.discoveryResponse()
				metadataEntries := 0
				if flags.nodes {
					metadataEntries = 1
					require.Contains(t, result[0], "nodes")
					require.NotContains(t, result[0], "address")
				}
				if flags.runners {
					require.Len(t, result, 2+metadataEntries)
				} else {
					require.Len(t, result, metadataEntries)
				}
				if !flags.nodes && !flags.runners {
					<-done
					require.Zero(t, calls.Load())
					return
				}
				require.Equal(t, int32(1), calls.Load())
				queried := snapshot().node.QueriedAt
				require.False(t, queried.IsZero())
				if flags.nodes {
					require.Len(t, s.discoveryResponse()[0]["nodes"], 1)
				}
				time.Sleep(5 * time.Minute)
				synctest.Wait()
				require.Equal(t, int32(2), calls.Load())
				require.True(t, snapshot().node.QueriedAt.After(queried))
				stall.Store(true)
				time.Sleep(5 * time.Minute)
				synctest.Wait()
				time.Sleep(3 * time.Second)
				synctest.Wait()
				require.Equal(t, 1, snapshot().failures)
				time.Sleep(5*time.Minute - 3*time.Second)
				synctest.Wait()
				cancel()
				synctest.Wait()
				<-done
				require.Equal(t, 1, snapshot().failures, "shutdown must not count as failure")
			})
		})
	}
}

func TestDiscoveryBoundedConcurrency(t *testing.T) {
	targets := make([]string, 12)
	for i := range targets {
		targets[i] = fmt.Sprintf("https://child%d.example", i)
	}
	s := discoveryTestServer(t, targets...)
	entered := make(chan struct{}, 12)
	release := make(chan struct{})
	var active, maxActive, calls atomic.Int32
	s.client.Transport = discoveryRoundTrip(func(r *http.Request) (*http.Response, error) {
		n := active.Add(1)
		for old := maxActive.Load(); n > old; old = maxActive.Load() {
			if maxActive.CompareAndSwap(old, n) {
				break
			}
		}
		calls.Add(1)
		entered <- struct{}{}
		<-release
		active.Add(-1)
		if r.URL.Host == "child0.example" {
			return nil, fmt.Errorf("offline")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("[]"))}, nil
	})
	done := make(chan struct{})
	go func() { defer close(done); s.refresh(t.Context()) }()
	for range 5 {
		<-entered
	}
	require.Equal(t, int32(5), active.Load())
	close(release)
	<-done
	require.Equal(t, int32(12), calls.Load())
	require.LessOrEqual(t, maxActive.Load(), int32(5))
	require.Equal(t, 1, s.snapshots[0].failures)
	require.Len(t, s.discoveryResponse()[0]["nodes"], 11)
}

func TestDiscoverySources(t *testing.T) {
	s := discoveryTestServer(t, "https://child.example/", "https://child.example")
	require.Len(t, s.urls, 1)
	encoded := discoveryTestServer(t, "https://child.example/deploy%2F/", "https://child.example/deploy%2F")
	require.Len(t, encoded.urls, 1)
	require.Equal(t, "https://child.example/deploy%2F", encoded.urls[0].String())
}

func TestDiscoveryFailureThreshold(t *testing.T) {
	for _, threshold := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			p := discoveryTestParams(t, "https://child.example")
			p.MaxFailedRefreshes = threshold
			s, err := NewServer(p, slog.New(slog.NewTextHandler(io.Discard, nil)))
			require.NoError(t, err)
			defer s.Close()
			status := http.StatusOK
			s.client.Transport = discoveryRoundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(childInventory))}, nil
			})
			s.refresh(t.Context())
			require.Len(t, s.discoveryResponse(), 3)
			status = http.StatusServiceUnavailable
			for failure := 1; failure <= max(1, threshold); failure++ {
				s.refresh(t.Context())
				require.Equal(t, failure < threshold, len(s.discoveryResponse()) > 1)
				require.Equal(t, failure >= threshold, s.snapshots[0].node.QueriedAt.IsZero())
			}
			require.Empty(t, s.discoveryResponse())
		})
	}
}

func TestDiscoveryPreservesWholeRecords(t *testing.T) {
	const record = `{"url":"https://orch.example/apps/runner/session","app":"test","observed_at":"2000-01-01T00:00:00.123+00:00","mode":"persistent","capacity":4,"capacity_used":1,"capacity_available":3,"gpu":{"id":"0","name":"H100","vram_mb":80000},"price_info":{"price":123456789012345678901234567890,"currency":"wei","unit":"seconds"},"extension":{"value":9007199254740993}}`
	s := discoveryTestServer(t, "https://child.example")
	s.client.Transport = discoveryRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`[{"address":"https://orch.example","runners":[` + record + `]}]`))}, nil
	})
	s.refresh(t.Context())
	for range 2 {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/discovery", nil))
		require.Equal(t, 200, w.Code)
		var result []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result, 2)
		require.NotContains(t, result[0], "address")
		require.Equal(t, `"https://orch.example"`, string(result[1]["address"]))
		// Compare raw JSON so large integers cannot be rounded through float64.
		require.Equal(t, "["+record+"]", string(result[1]["runners"]))
	}
}

func TestDiscoveryPrivateSourcesRequireGrants(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, childInventory)
	}))
	defer upstream.Close()
	for _, granted := range []bool{false, true} {
		t.Run(fmt.Sprint(granted), func(t *testing.T) {
			p := discoveryTestParams(t, upstream.URL)
			if !granted {
				p.Grants = nil
			}
			s, err := NewServer(p, slog.New(slog.NewTextHandler(io.Discard, nil)))
			require.NoError(t, err)
			defer s.Close()
			calls.Store(0)
			s.refresh(t.Context())
			require.Equal(t, granted, calls.Load() > 0)
			require.Equal(t, granted, len(s.discoveryResponse()) > 1)
		})
	}
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}
