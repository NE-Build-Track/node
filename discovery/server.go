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
	"sync"
	"time"

	"github.com/livepeer/node/destination"
	discovery "github.com/livepeer/node/discovery/shared"
)

type discoveryNode struct {
	URL       string    `json:"url"`
	QueriedAt time.Time `json:"queried_at"`
}
type nodeSnapshot struct {
	node     discoveryNode
	entries  []discoveryEntry
	failures int
}
type Server struct {
	logger                           *slog.Logger
	mux                              *http.ServeMux
	client                           *http.Client
	urls                             []*url.URL
	interval                         time.Duration
	maxFailures                      int
	advertiseNodes, advertiseRunners bool
	mu                               sync.RWMutex
	snapshots                        []nodeSnapshot
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
	s := &Server{logger: logger, mux: http.NewServeMux(),
		client: client, urls: urls, interval: p.RefreshInterval, maxFailures: max(1, p.MaxFailedRefreshes),
		advertiseNodes: p.AdvertiseNodes, advertiseRunners: p.AdvertiseRunners, snapshots: make([]nodeSnapshot, len(urls))}
	s.mux.HandleFunc("GET /discovery", s.discovery)
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) Close() { s.client.CloseIdleConnections() }

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
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		d.refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *Server) refresh(ctx context.Context) {
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range min(5, len(d.urls)) {
		workers.Go(func() {
			for i := range jobs {
				entries, err := d.fetch(ctx, d.urls[i])
				if ctx.Err() != nil {
					continue
				}
				d.mu.Lock()
				previous := &d.snapshots[i]
				if err == nil {
					*previous = nodeSnapshot{node: discoveryNode{URL: d.urls[i].String(), QueriedAt: time.Now().UTC()}, entries: entries}
				} else {
					previous.failures = min(previous.failures+1, d.maxFailures)
					if previous.failures >= d.maxFailures {
						previous.node = discoveryNode{}
						previous.entries = nil
					}
				}
				d.mu.Unlock()
				if err != nil {
					d.logger.WarnContext(ctx, "discovery refresh failed", "source", d.urls[i].String(), "error", err)
				}
			}
		})
	}
	defer workers.Wait()
	defer close(jobs)
	for i := range d.urls {
		select {
		case <-ctx.Done():
			return
		case jobs <- i:
		}
	}
}

func (d *Server) fetch(ctx context.Context, base *url.URL) ([]discoveryEntry, error) {
	endpoint := base.Clone()
	endpoint.Path += "/discovery"
	if endpoint.RawPath != "" {
		endpoint.RawPath += "/discovery"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := d.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discovery source returned HTTP %d", response.StatusCode)
	}
	data, err := discovery.Read(response.Body)
	if err != nil {
		return nil, err
	}
	return discovery.Decode[discoveryRunner](data, time.Now())
}

func (s *Server) discoveryResponse() []map[string]any {
	nodes := []discoveryNode{}
	entries := []discoveryEntry{}
	s.mu.RLock()
	for _, snapshot := range s.snapshots {
		if snapshot.node.QueriedAt.IsZero() {
			continue
		}
		if s.advertiseNodes {
			nodes = append(nodes, snapshot.node)
		}
		if s.advertiseRunners {
			entries = append(entries, snapshot.entries...)
		}
	}
	s.mu.RUnlock()
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
