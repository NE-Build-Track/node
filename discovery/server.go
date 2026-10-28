package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/livepeer/node/destination"
	discovery "github.com/livepeer/node/discovery/shared"
	"github.com/livepeer/node/internal/observability"
)

type discoveryNode struct {
	URL       string    `json:"url"`
	QueriedAt time.Time `json:"queried_at"`
}
type Server struct {
	metrics                          *observability.Metrics
	logger                           *slog.Logger
	mux                              *http.ServeMux
	urls                             []*url.URL
	interval                         time.Duration
	advertiseNodes, advertiseRunners bool
	collector                        *discovery.Collector[discoveryRunner]
}

func discoveryBases(p Params) ([]*url.URL, error) {
	urls := make([]*url.URL, 0, len(p.Nodes))
	seen := map[string]bool{}
	for _, item := range p.Nodes {
		if err := discovery.ValidateURL(item.Value); err != nil {
			return nil, err
		}
		u := item.Value.Clone()
		u.RawPath = strings.TrimRight(u.EscapedPath(), "/")
		u.Path, _ = url.PathUnescape(u.RawPath)
		if !seen[u.String()] {
			seen[u.String()] = true
			urls = append(urls, u)
		}
	}
	return urls, nil
}

// NewServer configures an inventory collector without runner or payment dependencies.
func NewServer(p Params, logger *slog.Logger) (*Server, error) {
	if !p.Listen.IsValid() {
		return nil, errors.New("--listen (or LIVEPEER_DISCOVERY_LISTEN) must be an IP:port address")
	}
	if !p.MetricsListen.IsValid() || !p.MetricsListen.Addr().IsLoopback() {
		return nil, errors.New("--metrics-listen (or LIVEPEER_DISCOVERY_METRICS_LISTEN) must bind a loopback IP:port address")
	}
	if p.RefreshInterval <= 0 {
		return nil, errors.New("--refresh-interval must be positive")
	}
	if p.MaxFailedRefreshes < 0 {
		return nil, errors.New("--max-failed-refreshes must be nonnegative")
	}
	urls, err := discoveryBases(p)
	if err != nil {
		return nil, err
	}
	policy, err := destination.New("discovery", p.Grants)
	if err != nil {
		return nil, fmt.Errorf("invalid --grants: %w", err)
	}
	client := policy.Client()
	client.Timeout = 3 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	metrics := observability.New("discovery")
	s := &Server{metrics: metrics, logger: logger.With("component", "discovery"), mux: http.NewServeMux(),
		urls: urls, interval: p.RefreshInterval,
		advertiseNodes: p.AdvertiseNodes, advertiseRunners: p.AdvertiseRunners,
		collector: &discovery.Collector[discoveryRunner]{Client: client, MaxFailures: max(1, p.MaxFailedRefreshes), Logger: logger.With("component", "discovery"), Observe: metrics.Discovery}}
	endpoints := make([]*url.URL, 0, len(urls))
	for _, base := range urls {
		endpoint := base.Clone()
		endpoint.Path += "/discovery"
		if endpoint.RawPath != "" {
			endpoint.RawPath += "/discovery"
		}
		endpoints = append(endpoints, endpoint)
	}
	s.collector.SetSources(endpoints)
	s.mux.HandleFunc("GET /discovery", s.discovery)
	metrics.Snapshot(s.collector.CollectMetrics)
	metrics.Legacy(func(emit observability.Emit) { emit("discovery_up", 1, nil) })
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.metrics.HTTP(w, r, func(w http.ResponseWriter, r *http.Request) {
		_, pattern := s.mux.Handler(r)
		observability.Operation(r, pattern)
		s.mux.ServeHTTP(w, r)
	})
}

func (s *Server) Close() { s.collector.Close() }

func (s *Server) discovery(w http.ResponseWriter, r *http.Request) {
	body, err := json.Marshal(s.discoveryResponse())
	if err != nil {
		http.Error(w, "cannot encode discovery inventory", http.StatusInternalServerError)
		return
	}
	body = append(body, '\n')
	if len(body) > discovery.MaxBody {
		s.logger.WarnContext(r.Context(), "discovery response exceeds signer size limit", "response_bytes", len(body), "max_bytes", discovery.MaxBody)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// Run refreshes immediately and periodically until ctx ends. Run it once and
// wait for it to return before closing the server.
func (d *Server) Run(ctx context.Context) {
	if (!d.advertiseNodes && !d.advertiseRunners) || len(d.urls) == 0 {
		return
	}
	d.collector.Run(ctx, d.interval)
}

func (s *Server) discoveryResponse() []map[string]any {
	nodes := []discoveryNode{}
	entries := []discoveryEntry{}
	for _, snapshot := range s.collector.Snapshots() {
		if s.advertiseNodes {
			nodes = append(nodes, discoveryNode{URL: strings.TrimSuffix(snapshot.URL, "/discovery"), QueriedAt: snapshot.QueriedAt})
		}
		if s.advertiseRunners {
			entries = append(entries, snapshot.Entries...)
		}
	}
	result := []map[string]any{}
	if len(nodes) > 0 {
		result = append(result, map[string]any{"nodes": nodes})
	}
	for _, entry := range discovery.Merge(entries) {
		if len(entry.Runners) == 0 {
			continue
		}
		result = append(result, map[string]any{"address": entry.Address, "runners": entry.Runners})
	}
	return result
}

// discoveryRunner retains the full upstream record, including extension fields
// and exact numeric values, while exposing the identity used by the shared codec.
type discoveryRunner struct {
	URL        string `json:"url"`
	App        string `json:"app"`
	ObservedAt string `json:"observed_at"`
	data       json.RawMessage
}

func (r *discoveryRunner) UnmarshalJSON(data []byte) error {
	type identity discoveryRunner
	var value identity
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*r = discoveryRunner(value)
	r.data = append(json.RawMessage(nil), data...)
	return nil
}

func (r discoveryRunner) MarshalJSON() ([]byte, error) { return r.data, nil }

func (r discoveryRunner) DiscoveryIdentity() (string, string, string) {
	return r.URL, r.App, r.ObservedAt
}

type discoveryEntry = discovery.Entry[discoveryRunner]
