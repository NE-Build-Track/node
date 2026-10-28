package shared

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

type collectorTransport func(*http.Request) (*http.Response, error)

func (f collectorTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func collectorResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
}

func TestCollectorCacheLifecycle(t *testing.T) {
	status, body := 200, envelope(candidate("1999-01-01T00:00:00Z"))
	c := &Collector[record]{MaxFailures: 6, Client: &http.Client{Transport: collectorTransport(func(*http.Request) (*http.Response, error) { return collectorResponse(status, body), nil })}}
	c.SetSources([]*url.URL{{Scheme: "https", Host: "source.example"}})
	m := observability.New("discovery")
	m.Snapshot(c.CollectMetrics)
	assertMetrics := func(state string, count, failures int, last time.Time) {
		t.Helper()
		require.Len(t, test.MetricSamples(t, m.Registry, "discovery_source_state", nil), 4)
		for _, candidate := range []string{"uninitialized", "healthy", "retained", "evicted"} {
			require.Equal(t, observability.Bool(candidate == state), test.MetricSum(t, m.Registry, "discovery_source_state", map[string]string{"state": candidate}))
		}
		wantLast := 0.0
		if !last.IsZero() {
			wantLast = float64(last.Unix())
		}
		require.Equal(t, wantLast, test.MetricSum(t, m.Registry, "discovery_last_success_timestamp_seconds", nil))
		require.EqualValues(t, count, test.MetricSum(t, m.Registry, "discovery_cached_runners", nil))
		require.EqualValues(t, failures, test.MetricSum(t, m.Registry, "discovery_source_failures", nil))
	}
	assertMetrics("uninitialized", 0, 0, time.Time{})
	status = 500
	c.Refresh(t.Context(), false)
	assertMetrics("uninitialized", 0, 1, time.Time{})
	status = 200
	c.Refresh(t.Context(), false)
	original := c.Snapshots()[0]
	assertMetrics("healthy", 1, 0, original.LastSuccess)
	for i, failure := range []struct {
		status int
		body   string
	}{
		{500, body}, {200, "bad JSON"}, {200, envelope(`{}`)}, {200, strings.Repeat(" ", MaxBody+1)}, {302, body},
	} {
		status, body = failure.status, failure.body
		c.Refresh(t.Context(), false)
		got := c.Snapshots()[0]
		require.Equal(t, original.Entries, got.Entries)
		require.Equal(t, original.LastSuccess, got.LastSuccess)
		assertMetrics("retained", 1, i+1, original.LastSuccess)
		require.Equal(t, original.QueriedAt, got.QueriedAt)
		require.Equal(t, i+1, got.Failures)
	}
	c.Refresh(t.Context(), false)
	require.Empty(t, c.Snapshots())
	assertMetrics("evicted", 0, 6, original.LastSuccess)
	require.True(t, c.Initialized(), "eviction does not reset startup")
	status, body = 200, envelope(candidate("1999-02-01T00:00:00Z"))
	c.Refresh(t.Context(), false)
	require.Equal(t, "1999-02-01T00:00:00Z", c.Snapshots()[0].Entries[0].Runners[0].Observed)
	require.Zero(t, c.Snapshots()[0].Failures)
	assertMetrics("healthy", 1, 0, c.Snapshots()[0].LastSuccess)
	status = 500
	c.Refresh(t.Context(), false)
	status, body = 200, "[]"
	c.Refresh(t.Context(), false)
	require.Len(t, c.Snapshots(), 1, "empty success is still a usable source")
	require.Empty(t, c.Snapshots()[0].Entries)
	assertMetrics("healthy", 0, 0, c.Snapshots()[0].LastSuccess)
	require.Zero(t, c.Snapshots()[0].Failures)
}

func TestCollectorSourceChangesDuringRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := &url.URL{Scheme: "https", Host: "a.example"}, &url.URL{Scheme: "https", Host: "b.example"}
		release := make(chan struct{})
		var aCalls, bCalls int
		c := &Collector[record]{MaxFailures: 6, Client: &http.Client{Transport: collectorTransport(func(r *http.Request) (*http.Response, error) {
			if r.URL.Host == b.Host {
				bCalls++
				return collectorResponse(200, "[]"), nil
			}
			aCalls++
			if aCalls == 1 {
				return collectorResponse(500, ""), nil
			}
			<-release
			return collectorResponse(200, "[]"), nil
		})}}
		c.SetSources([]*url.URL{a})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go c.Run(ctx, time.Hour)
		synctest.Wait()
		go c.Refresh(ctx, true)
		synctest.Wait()
		c.SetSources([]*url.URL{a, b, a})
		synctest.Wait()
		close(release)
		synctest.Wait()
		require.Equal(t, 1, bCalls, "source added during a joined refresh is fetched immediately")
		original := c.Snapshots()
		aBefore := aCalls
		c.SetSources([]*url.URL{a, b, a})
		synctest.Wait()
		require.Equal(t, aBefore, aCalls, "identical duplicate list must not poll again")
		require.Equal(t, 1, bCalls)
		require.Equal(t, original, c.Snapshots())
		c.SetSources([]*url.URL{b, a})
		synctest.Wait()
		require.Equal(t, []Snapshot[record]{original[1], original[0]}, c.Snapshots(), "reordering preserves observations and changes precedence")
		c.SetSources([]*url.URL{b})
		synctest.Wait()
		require.Equal(t, original[1:], c.Snapshots(), "retained snapshots survive removal of other sources")
		require.Equal(t, aBefore, aCalls, "reordering or removing sources does not poll again")
		require.Equal(t, 1, bCalls)
	})
}

func TestCollectorRemovedSourceCannotReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		source := &url.URL{Scheme: "https", Host: "source.example"}
		c := &Collector[record]{MaxFailures: 6, Client: &http.Client{Transport: collectorTransport(func(*http.Request) (*http.Response, error) {
			<-release
			return collectorResponse(200, "[]"), nil
		})}}
		c.SetSources([]*url.URL{source})
		go c.Refresh(t.Context(), false)
		synctest.Wait()
		c.SetSources(nil)
		c.SetSources([]*url.URL{source})
		close(release)
		synctest.Wait()
		require.Empty(t, c.Snapshots(), "removed and re-added source cannot reuse an obsolete in-flight result")
		require.False(t, c.Initialized())
	})
}

func TestCollectorFailureDiagnostics(t *testing.T) {
	policy, err := destination.New("discovery", nil)
	require.NoError(t, err)
	policy.Lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return nil, &net.DNSError{Err: "no such host", Name: "missing.example", IsNotFound: true}
	}
	for _, test := range []struct {
		name, endpoint, want string
		transport            http.RoundTripper
	}{
		{"grant", "http://127.0.0.1/private-token", "denied_destination", policy.Client().Transport},
		{"DNS", "https://missing.example/private-token", "dns", policy.Client().Transport},
		{"timeout", "https://source.example/private-token", "timeout", collectorTransport(func(*http.Request) (*http.Response, error) { return nil, context.DeadlineExceeded })},
		{"redirect", "https://source.example/private-token", "invalid_redirect", collectorTransport(func(*http.Request) (*http.Response, error) {
			response := collectorResponse(302, "")
			response.Header = http.Header{"Location": {"https://redirect.example/private-token/%zz"}}
			return response, nil
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			c := &Collector[record]{Client: &http.Client{Transport: test.transport}, Logger: slog.New(slog.NewTextHandler(&logs, nil))}
			defer c.Close()
			endpoint, err := url.Parse(test.endpoint)
			require.NoError(t, err)
			c.SetSources([]*url.URL{endpoint})
			c.Refresh(t.Context(), false)
			require.Contains(t, logs.String(), "source_index=0")
			require.Contains(t, logs.String(), test.want)
			require.NotContains(t, logs.String(), "private-token")
			require.NotContains(t, logs.String(), "https://")
			require.NotContains(t, logs.String(), "http://")
		})
	}
}
