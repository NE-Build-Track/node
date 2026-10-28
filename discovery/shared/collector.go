package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/internal/observability"
)

// Snapshot is an immutable successful inventory observation.
type Snapshot[R Runner] struct {
	URL         string
	QueriedAt   time.Time
	Entries     []Entry[R]
	Failures    int
	LastSuccess time.Time
}

// Collector polls complete discovery endpoints and retains per-source inventories.
// Configure the client and failure limit before starting workers.
type Collector[R Runner] struct {
	Client         *http.Client
	Logger         *slog.Logger
	MaxFailures    int
	Observe        func(int, time.Duration, error)
	mu             sync.Mutex
	sources        []*Snapshot[R]
	pending        chan struct{}
	cancel         context.CancelFunc
	closed         bool
	initialized    bool
	sourcesChanged bool
	wake           chan struct{}
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
	added := false
	for _, u := range urls {
		key := u.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		s := old[key]
		if s == nil {
			s = &Snapshot[R]{URL: key}
			added = true
		}
		next = append(next, s)
	}
	c.sources = next
	if added {
		c.sourcesChanged = true
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

// Refresh coalesces fetches owned by the collector; any caller may stop waiting.
func (c *Collector[R]) Refresh(ctx context.Context, initialOnly bool) {
	c.mu.Lock()
	if c.closed || ctx.Err() != nil || (initialOnly && c.initialized) {
		c.mu.Unlock()
		return
	}
	if c.pending == nil {
		fetchCtx, cancel := context.WithCancel(context.Background())
		c.cancel = cancel
		c.pending = make(chan struct{})
		c.sourcesChanged = false
		go c.refresh(fetchCtx, slices.Clone(c.sources))
	}
	pending := c.pending
	c.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-pending:
	}
}

// Close cancels and joins pending work, and prevents further refreshes.
func (c *Collector[R]) Close() {
	c.mu.Lock()
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	pending := c.pending
	c.mu.Unlock()
	if pending != nil {
		<-pending
	}
	if c.Client != nil {
		c.Client.CloseIdleConnections()
	}
}

func (c *Collector[R]) refresh(ctx context.Context, sources []*Snapshot[R]) {
	defer func() { c.mu.Lock(); c.cancel(); close(c.pending); c.pending = nil; c.mu.Unlock() }()
	slots := make(chan struct{}, 5)
	var workers sync.WaitGroup
feedLoop:
	for index, source := range sources {
		select {
		case <-ctx.Done():
			break feedLoop
		case slots <- struct{}{}:
		}
		workers.Go(func() {
			defer func() { <-slots }()
			started := time.Now()
			entries, err := c.fetch(ctx, source.URL)
			if c.Observe != nil {
				c.Observe(index, time.Since(started), err)
			}
			if ctx.Err() != nil {
				return
			}
			c.mu.Lock()
			if slices.Contains(c.sources, source) {
				if err == nil {
					source.Entries = entries
					source.QueriedAt = time.Now().UTC()
					source.LastSuccess = source.QueriedAt
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
				kind := observability.ErrorKind(err)
				var dns *net.DNSError
				if errors.Is(err, destination.ErrDeniedAddress) {
					kind = "denied_destination"
				} else if errors.As(err, &dns) {
					kind = "dns"
				} else if errors.Is(err, errInvalidRedirect) {
					kind = "invalid_redirect"
				}
				logger.WarnContext(ctx, "discovery refresh failed", "event", "discovery_refresh", "operation", "refresh", "outcome", "error", "source_index", index, "error_kind", kind)
			}
		})
	}
	workers.Wait()
}

var errInvalidRedirect = errors.New("invalid discovery redirect URL")

func (c *Collector[R]) fetch(ctx context.Context, endpoint string) ([]Entry[R], error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid discovery request")
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		var requestErr *url.Error
		if errors.As(err, &requestErr) {
			err = requestErr.Err
		}
		// Go includes the raw Location URL in this error, even after unwrapping.
		if strings.HasPrefix(err.Error(), "failed to parse Location header ") {
			err = errInvalidRedirect
		}
		return nil, err
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

// Run polls immediately, on source additions, and periodically, then closes the collector.
func (c *Collector[R]) Run(ctx context.Context, interval time.Duration) {
	defer c.Close()
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
		// A joined refresh may have captured its sources before the wakeup.
		c.mu.Lock()
		changed := c.sourcesChanged
		c.mu.Unlock()
		if changed {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
	}
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
