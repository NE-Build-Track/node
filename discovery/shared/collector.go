package shared

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Snapshot is an immutable successful inventory observation.
type Snapshot[R Runner] struct {
	URL       string
	QueriedAt time.Time
	Entries   []Entry[R]
	Failures  int
}

// Collector polls complete discovery endpoints and retains per-source inventories.
// Configure the client and failure limit before starting workers.
type Collector[R Runner] struct {
	Client      *http.Client
	Logger      *slog.Logger
	MaxFailures int
	mu          sync.Mutex
	sources     []*Snapshot[R]
	pending     chan struct{}
	initialized bool
	wake        chan struct{}
}

// SetSources preserves retained sources in input order and removes deleted ones.
func (c *Collector[R]) SetSources(urls []*url.URL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wake == nil {
		c.wake = make(chan struct{}, 1)
	}
	old := make(map[string]*Snapshot[R])
	for _, s := range c.sources {
		old[s.URL] = s
	}
	next := make([]*Snapshot[R], 0, len(urls))
	seen := map[string]bool{}
	changed := len(urls) != len(c.sources)
	for i, u := range urls {
		key := u.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		s := old[key]
		if s == nil {
			s = &Snapshot[R]{URL: key}
			changed = true
		}
		if i >= len(c.sources) || c.sources[i].URL != key {
			changed = true
		}
		next = append(next, s)
	}
	c.sources = next
	if changed {
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
}

func (c *Collector[R]) Initialized() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.initialized }

// Snapshots returns successful observations in source order. Entries are read-only.
func (c *Collector[R]) Snapshots() []Snapshot[R] {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := []Snapshot[R]{}
	for _, s := range c.sources {
		if !s.QueriedAt.IsZero() {
			out = append(out, *s)
		}
	}
	return out
}

// Refresh coalesces concurrent refreshes; canceling a waiter leaves the owner running.
func (c *Collector[R]) Refresh(ctx context.Context, initialOnly bool) {
	c.mu.Lock()
	if initialOnly && c.initialized {
		c.mu.Unlock()
		return
	}
	if pending := c.pending; pending != nil {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-pending:
		}
		return
	}
	c.pending = make(chan struct{})
	sources := append([]*Snapshot[R](nil), c.sources...)
	c.mu.Unlock()
	defer func() { c.mu.Lock(); close(c.pending); c.pending = nil; c.mu.Unlock() }()
	jobs := make(chan *Snapshot[R])
	var workers sync.WaitGroup
	for range min(5, len(sources)) {
		workers.Go(func() {
			for source := range jobs {
				entries, err := c.fetch(ctx, source.URL)
				if ctx.Err() != nil {
					continue
				}
				c.mu.Lock()
				retained := false
				for _, current := range c.sources {
					if current == source {
						retained = true
						break
					}
				}
				if retained {
					if err == nil {
						source.Entries = entries
						source.QueriedAt = time.Now().UTC()
						source.Failures = 0
						c.initialized = true
					} else {
						source.Failures = min(source.Failures+1, c.MaxFailures)
						if source.Failures >= c.MaxFailures {
							source.Entries = nil
							source.QueriedAt = time.Time{}
						}
					}
				}
				c.mu.Unlock()
				if err != nil {
					logger := c.Logger
					if logger == nil {
						logger = slog.Default()
					}
					logger.WarnContext(ctx, "discovery refresh failed", "error", err)
				}
			}
		})
	}
feedLoop:
	for _, s := range sources {
		select {
		case <-ctx.Done():
			break feedLoop
		case jobs <- s:
		}
	}
	close(jobs)
	workers.Wait()
}

func (c *Collector[R]) fetch(ctx context.Context, endpoint string) ([]Entry[R], error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid discovery request")
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("discovery request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("discovery source returned HTTP %d", resp.StatusCode)
	}
	data, err := Read(resp.Body)
	if err != nil {
		return nil, err
	}
	return Decode[R](data, time.Now())
}

// Run polls immediately, on source changes, and at the supplied interval.
func (c *Collector[R]) Run(ctx context.Context, interval time.Duration) {
	c.mu.Lock()
	if c.wake == nil {
		c.wake = make(chan struct{}, 1)
	}
	wake := c.wake
	c.mu.Unlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		select {
		case <-wake:
		default:
		}
		c.Refresh(ctx, false)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
	}
}

// State returns all source states, including sources without a usable inventory.
func (c *Collector[R]) State() []Snapshot[R] {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Snapshot[R], 0, len(c.sources))
	for _, s := range c.sources {
		out = append(out, *s)
	}
	return out
}

// AcceptEmpty marks a successful empty source selection as initialized.
// Later source additions are collected by the worker rather than HTTP requests.
func (c *Collector[R]) AcceptEmpty() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sources) == 0 {
		c.initialized = true
	}
}
