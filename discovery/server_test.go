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
	discovery "github.com/livepeer/node/discovery/shared"
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

func TestDiscoverySourcePolicy(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "/deployment/discovery", r.URL.Path)
		require.Empty(t, r.URL.RawQuery)
		if calls.Load() > 1 {
			http.Redirect(w, r, "/unconfigured/discovery", http.StatusFound)
			return
		}
		io.WriteString(w, strings.ReplaceAll(childInventory, "http://127.0.0.1:1", "http://"+r.Host+"/unconfigured"))
	}))
	defer upstream.Close()
	s := discoveryTestServer(t, upstream.URL+"/deployment")
	s.collector.MaxFailures = 1
	s.collector.Refresh(t.Context(), false)
	require.Len(t, s.discoveryResponse(), 3)
	require.Equal(t, upstream.URL+"/deployment", s.discoveryResponse()[0]["nodes"].([]discoveryNode)[0].URL)
	s.collector.Refresh(t.Context(), false)
	require.Empty(t, s.discoveryResponse())
	require.Equal(t, int32(2), calls.Load(), "neither redirects nor advertised nodes may be followed")
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
				snapshot := func() discovery.Snapshot[discoveryRunner] { return s.collector.Snapshots()[0] }
				var calls atomic.Int32
				var stall atomic.Bool
				s.collector.Client.Transport = discoveryRoundTrip(func(r *http.Request) (*http.Response, error) {
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
				queried := snapshot().QueriedAt
				require.False(t, queried.IsZero())
				if flags.nodes {
					require.Len(t, s.discoveryResponse()[0]["nodes"], 1)
				}
				time.Sleep(5 * time.Minute)
				synctest.Wait()
				require.Equal(t, int32(2), calls.Load())
				require.True(t, snapshot().QueriedAt.After(queried))
				stall.Store(true)
				time.Sleep(5 * time.Minute)
				synctest.Wait()
				time.Sleep(3 * time.Second)
				synctest.Wait()
				require.Equal(t, 1, snapshot().Failures)
				time.Sleep(5*time.Minute - 3*time.Second)
				synctest.Wait()
				cancel()
				synctest.Wait()
				<-done
				require.Equal(t, 1, snapshot().Failures, "shutdown must not count as failure")
			})
		})
	}
}

func TestDiscoveryBoundedConcurrencyAndPartialResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		targets := make([]string, 12)
		for i := range targets {
			targets[i] = fmt.Sprintf("https://child%d.example", i)
		}
		s := discoveryTestServer(t, targets...)
		release, slow := make(chan struct{}), make(chan struct{})
		var active, maxActive, calls atomic.Int32
		s.collector.Client.Transport = discoveryRoundTrip(func(r *http.Request) (*http.Response, error) {
			n := active.Add(1)
			for old := maxActive.Load(); n > old; old = maxActive.Load() {
				if maxActive.CompareAndSwap(old, n) {
					break
				}
			}
			calls.Add(1)
			<-release
			if r.URL.Host == "child0.example" {
				<-slow
			}
			active.Add(-1)
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("[]"))}, nil
		})
		go s.collector.Refresh(t.Context(), false)
		synctest.Wait()
		require.Equal(t, int32(5), active.Load())
		close(release)
		synctest.Wait()
		partial := s.discoveryResponse()
		close(slow)
		synctest.Wait()
		require.NotEmpty(t, partial)
		require.Len(t, partial[0]["nodes"], 11, "completed sources are visible while another source stalls")
		require.Equal(t, int32(12), calls.Load())
		require.LessOrEqual(t, maxActive.Load(), int32(5))
		require.Len(t, s.discoveryResponse()[0]["nodes"], 12)
	})
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
			s.collector.Client.Transport = discoveryRoundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(childInventory))}, nil
			})
			s.collector.Refresh(t.Context(), false)
			require.Len(t, s.discoveryResponse(), 3)
			status = http.StatusServiceUnavailable
			for failure := 1; failure <= max(1, threshold); failure++ {
				s.collector.Refresh(t.Context(), false)
				require.Equal(t, failure < threshold, len(s.discoveryResponse()) > 1)
				require.Equal(t, failure >= threshold, len(s.collector.Snapshots()) == 0)
			}
			require.Empty(t, s.discoveryResponse())
		})
	}
}

func TestDiscoveryPreservesWholeRecords(t *testing.T) {
	const record = `{"url":"https://orch.example/apps/runner/session","app":"test","observed_at":"2000-01-01T00:00:00.123+00:00","mode":"persistent","capacity":4,"capacity_used":1,"capacity_available":3,"gpu":{"id":"0","name":"H100","vram_mb":80000},"price_info":{"price":123456789012345678901234567890,"currency":"wei","unit":"seconds"},"extension":{"value":9007199254740993}}`
	s := discoveryTestServer(t, "https://child.example")
	s.collector.Client.Transport = discoveryRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`[{"address":"https://orch.example","runners":[` + record + `]}]`))}, nil
	})
	s.collector.Refresh(t.Context(), false)
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
			s.collector.Refresh(t.Context(), false)
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
