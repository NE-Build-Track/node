package shared

import (
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type collectorTransport func(*http.Request) (*http.Response, error)

func (f collectorTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCollectorRemovedSourceCannotReturn(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	c := &Collector[record]{MaxFailures: 6, Client: &http.Client{Transport: collectorTransport(func(*http.Request) (*http.Response, error) {
		close(started)
		<-release
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(envelope(candidate("2026-01-01T00:00:00Z"))))}, nil
	})}}
	source, _ := url.Parse("https://source.example/discovery")
	c.SetSources([]*url.URL{source})
	done := make(chan struct{})
	go func() { defer close(done); c.Refresh(t.Context(), false) }()
	<-started
	c.SetSources(nil)
	// A canceled waiter must leave the shared request running.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c.Refresh(ctx, false)
	require.Empty(t, c.Snapshots())
	close(release)
	<-done
	require.Empty(t, c.Snapshots())
	require.False(t, c.Initialized())
}
