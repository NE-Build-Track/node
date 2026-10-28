package signer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	discovery "github.com/livepeer/node/discovery/shared"
	"github.com/livepeer/node/internal/observability"
)

type orchestratorWebhook struct {
	changed  func([]*url.URL)
	endpoint *url.URL
	headers  http.Header
	interval time.Duration
	urls     atomic.Pointer[[]*url.URL]

	mu         sync.Mutex
	refreshing chan struct{}
}

func validateOrchestratorWebhook(endpoint *url.URL, headers Headers, interval time.Duration) error {
	if err := validateAuthWebhook(endpoint); err != nil {
		return errors.New("orchestrator webhook URL (LIVEPEER_SIGNER_ORCHESTRATOR_WEBHOOK or --orchestrator-webhook-file) must be an absolute HTTP or HTTPS URL without credentials or fragment")
	}
	if _, err := headers.MarshalText(); err != nil {
		return fmt.Errorf("invalid orchestrator webhook headers (LIVEPEER_SIGNER_ORCHESTRATOR_WEBHOOK_HEADERS or --orchestrator-webhook-headers-file): %w", err)
	}
	if interval <= 0 {
		return errors.New("--orchestrator-webhook-refresh-interval must be positive")
	}
	return nil
}

// SetOrchestratorWebhook replaces static discovery sources with a cached webhook
// list. Call SetDiscovery first to configure destination grants, and configure
// the service before starting RunOrchestratorWebhook or serving requests.
func (s *Service) SetOrchestratorWebhook(endpoint *url.URL, headers Headers, refreshInterval time.Duration) error {
	if err := validateOrchestratorWebhook(endpoint, headers, refreshInterval); err != nil {
		return err
	}
	if s.collector.Client == nil {
		return errors.New("configure discovery grants with SetDiscovery before the orchestrator webhook")
	}
	h := &orchestratorWebhook{
		endpoint: endpoint.Clone(),
		headers:  http.Header(headers).Clone(), interval: refreshInterval,
	}
	h.changed = func(urls []*url.URL) {
		s.collector.SetSources(urls)
		if len(urls) == 0 {
			s.collector.AcceptEmpty()
		}
	}
	s.collector.SetSources(nil)
	s.orchWebhook = h
	return nil
}

// RunOrchestratorWebhook refreshes immediately and periodically until ctx ends.
// Run it once after configuration, and wait for it to return before Close.
func (s *Service) RunOrchestratorWebhook(ctx context.Context) {
	h := s.orchWebhook
	if h == nil {
		return
	}
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		h.refresh(ctx, s.collector.Client.Transport, false)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Initial discovery requests share a fetch with each other and the worker.
// A successful snapshot, including an empty list, ends lazy startup retries.
func (h *orchestratorWebhook) refresh(ctx context.Context, transport http.RoundTripper, initialOnly bool) {
	h.mu.Lock()
	if initialOnly && h.urls.Load() != nil {
		h.mu.Unlock()
		return
	}
	if pending := h.refreshing; pending != nil {
		h.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-pending:
		}
		return
	}
	h.refreshing = make(chan struct{})
	h.mu.Unlock()
	urls, err := h.fetch(ctx, transport)
	if err == nil {
		h.urls.Store(&urls)
		if h.changed != nil {
			h.changed(urls)
		}
	} else if ctx.Err() == nil {
		// fetch never returns a URL, credentials, or response body in errors.
		slog.With("component", "signer", "event", "orchestrator_webhook", "operation", "orchestrator_webhook").WarnContext(ctx, "signer orchestrator webhook refresh failed; retaining cached list", "outcome", "error", "error_kind", observability.ErrorKind(err))
	}
	h.mu.Lock()
	close(h.refreshing)
	h.refreshing = nil
	h.mu.Unlock()
}

func (h *orchestratorWebhook) fetch(ctx context.Context, transport http.RoundTripper) ([]*url.URL, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.endpoint.String(), nil)
	if err != nil {
		return nil, errors.New("orchestrator webhook request construction failed")
	}
	request.Header = h.headers.Clone()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("orchestrator webhook request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("orchestrator webhook returned HTTP status %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return nil, errors.New("orchestrator webhook response unreadable or exceeds 1 MiB")
	}
	var entries []struct{ Address string }
	if err := json.Unmarshal(data, &entries); err != nil || entries == nil {
		return nil, errors.New("orchestrator webhook response must be a JSON array of address objects")
	}
	urls := make([]*url.URL, 0, len(entries))
	for _, entry := range entries {
		candidate, err := url.Parse(entry.Address)
		if err == nil && discovery.ValidateURL(candidate) == nil {
			urls = append(urls, candidate)
		}
	}
	return urls, nil
}
