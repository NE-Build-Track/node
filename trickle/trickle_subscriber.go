package trickle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/livepeer/node/internal/observability"
)

type SequenceStart int

const (
	Current SequenceStart = -2
	Next    SequenceStart = -1
)

// TrickleSubscriberConfig holds all NewTrickleSubscriber inputs.
// Pass this by value; any nil or zero will fall back to defaults.
type TrickleSubscriberConfig struct {

	// Trickle URL to subscribe to (required).
	URL string

	// InsecureSkipVerify skips TLS certificate validation when true.
	// Defaults to false.
	InsecureSkipVerify bool

	// Pass in a context for custom cancellation of
	// the entire subscription.
	//
	// Setting a context here is unusual but aligns
	// better with how contexts are used internally
	// (eg, they do not strictly map to a single Read request)
	Ctx context.Context

	// Set the index of the first sequence to read.
	// Pointer to distinguish unset from a valid zero field.
	Start *SequenceStart
}

//lint:ignore ST1012 Preserve the public go-livepeer trickle API.
var EOS = errors.New("end of stream")

type SequenceNonexistent struct {
	Latest int
	Seq    int
}

func (e *SequenceNonexistent) Error() string {
	return fmt.Sprintf("Channel exists but sequence does not: requested %d latest %d", e.Seq, e.Latest)
}

const preconnectRefreshTimeout = 20 * time.Second

var errPreconnectTimeout = errors.New("preconnect timed out")

// TrickleSubscriber represents a trickle streaming reader that always fetches from index -1
type TrickleSubscriber struct {
	client             *http.Client
	url                string
	mu                 sync.Mutex      // Mutex to manage concurrent access
	pendingGet         *http.Response  // Pre-initialized GET request
	baseCtx            context.Context // base context to use for the next context
	ctx                context.Context // Parent context to use for pending GETs. This is bad
	cancelCtx          func()          // cancel the pending GET
	idx                int             // Segment index to request
	insecureSkipVerify bool

	// Number of errors from preconnect
	preconnectErrorCount int
}

// NewTrickleSubscriber creates a new trickle stream reader for GET requests
func NewTrickleSubscriber(config TrickleSubscriberConfig) (*TrickleSubscriber, error) {

	if config.URL == "" {
		return nil, errors.New("trickle subscription URL missing")
	}

	// Context + cancel
	baseCtx := config.Ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(baseCtx)

	// Starting index
	idx := int(Next)
	if config.Start != nil {
		idx = int(*config.Start)
	}

	return &TrickleSubscriber{
		client:             httpClient(config.InsecureSkipVerify),
		url:                config.URL,
		baseCtx:            baseCtx,
		ctx:                ctx,
		cancelCtx:          cancel,
		idx:                idx,
		insecureSkipVerify: config.InsecureSkipVerify,
	}, nil
}

func GetSeq(resp *http.Response) int {
	if resp == nil {
		return -99 // TODO hmm
	}
	v := resp.Header.Get("Lp-Trickle-Seq")
	i, err := strconv.Atoi(v)
	if err != nil {
		// Fetch the latest index
		// TODO think through whether this is desirable
		return -98
	}
	return i
}

func GetLatest(resp *http.Response) int {
	if resp == nil {
		return -99 // TODO hmm
	}
	v := resp.Header.Get("Lp-Trickle-Latest")
	i, err := strconv.Atoi(v)
	if err != nil {
		return -1 // Use the latest index on the server
	}
	return i
}

func IsEOS(resp *http.Response) bool {
	return resp.Header.Get("Lp-Trickle-Closed") != ""
}

func (c *TrickleSubscriber) SetSeq(seq int) {
	// cancel this outside the lock since we may be deadlocked in preconect otherwise
	// not super safe on paper but OK in practice, just don't call SetSeq concurrently
	c.cancelCtx()
	c.mu.Lock()
	defer c.mu.Unlock()
	// Reset client in case SetSeq is called due to slow reads falling too far behind
	c.client = httpClient(c.insecureSkipVerify)
	c.idx = seq
	c.ctx, c.cancelCtx = context.WithCancel(c.baseCtx)
	c.pendingGet = nil
	c.preconnectErrorCount = 0
}

func (c *TrickleSubscriber) connect(ctx context.Context) (*http.Response, error) {
	url := fmt.Sprintf("%s/%d", c.url, c.idx)
	slog.With("component", "trickle", "event", "stream", "operation", "stream").Debug("preconnecting")

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		slog.With("component", "trickle", "event", "stream", "operation", "stream").Error("Failed to create request for segment", "outcome", "error", "error_kind", observability.ErrorKind(err))
		return nil, err
	}

	// Execute the GET request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to complete GET for next segment: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close() // Ensure we close the body to avoid leaking connections
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == 470 {
			return resp, nil
		}
		return nil, fmt.Errorf("failed GET segment, status code: %d, msg: %s", resp.StatusCode, string(body))
	}

	// Return the pre-initialized GET request
	return resp, nil
}

// preconnect pre-initializes the next GET request for fetching the next segment
// This blocks until headers are received  as soon as data is ready.
// If blocking takes a while, it re-creates the connection every so often.
func (c *TrickleSubscriber) preconnect() (*http.Response, error) {
	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	runConnect := func(ctx context.Context) {
		go func() {
			resp, err := c.connect(ctx)
			if err != nil {
				if errors.Is(err, errPreconnectTimeout) {
					// cancelled as part of a preconnect refresh, so ignore
					return
				}
				errCh <- err
				return
			}
			respCh <- resp
		}()
	}
	ctx, cancel := context.WithCancelCause(c.ctx)
	runConnect(ctx)
	for {
		select {
		case err := <-errCh:
			cancel(nil)
			return nil, err
		case resp := <-respCh:
			if resp.StatusCode == http.StatusOK {
				resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
			} else {
				cancel(nil)
			}
			return resp, nil
		case <-time.After(preconnectRefreshTimeout):
			// Use a custom error for the timeout to avoid clashes with parent cancellations
			// Not doing so could lead to a deadlock due to runConnect returning nothing
			cancel(errPreconnectTimeout)
			ctx, cancel = context.WithCancelCause(c.ctx)
			runConnect(ctx)
		}
	}
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelCauseFunc
}

func (body *cancelOnClose) Close() error {
	err := body.ReadCloser.Close()
	body.cancel(nil)
	return err
}

// Read retrieves data from the current segment and sets up the next segment concurrently.
// It returns the reader for the current segment's data.
func (c *TrickleSubscriber) Read() (*http.Response, error) {

	// Acquire lock to manage access to pendingGet
	// Blocking is intentional if there is no preconnect
	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO clean up this preconnect error handling!
	hitMaxPreconnects := c.preconnectErrorCount > 5
	if hitMaxPreconnects {
		slog.With("component", "trickle", "event", "stream", "operation", "stream").Error("Hit max preconnect error", "idx", c.idx)
		return nil, fmt.Errorf("hit max preconnects")
	}

	// Get the reader to use for the current segment
	conn := c.pendingGet
	if conn == nil {
		// Preconnect if we don't have a pending GET
		slog.With("component", "trickle", "event", "stream", "operation", "stream").Debug("No preconnect, connecting", "idx", c.idx)
		p, err := c.preconnect()
		if err != nil {
			c.preconnectErrorCount++
			return nil, err
		}
		conn = p
		// reset preconnect error
		c.preconnectErrorCount = 0
	}
	c.pendingGet = nil

	if IsEOS(conn) {
		conn.Body.Close() // because this is a 200; maybe use a custom status code
		return nil, EOS
	}

	if conn.StatusCode == http.StatusNotFound {
		return nil, StreamNotFoundErr
	}

	if conn.StatusCode == 470 {
		// stream exists but segment dosn't
		return nil, &SequenceNonexistent{Seq: GetSeq(conn), Latest: GetLatest(conn)}
	}

	// Set to use the next index for the next (pre-)connection
	idx := GetSeq(conn)
	if idx >= 0 {
		c.idx = idx + 1
	}

	// Set up the next connection
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		nextConn, err := c.preconnect()
		if err != nil {
			slog.With("component", "trickle", "event", "stream", "operation", "stream").Error("failed to preconnect next segment", "idx", c.idx, "outcome", "error", "error_kind", observability.ErrorKind(err))
			c.preconnectErrorCount++
			return
		}

		c.pendingGet = nextConn

		// reset preconnect error
		c.preconnectErrorCount = 0
	}()

	// Now the segment is set up and we have the reader for the current one

	// Return the reader for the current segment
	return conn, nil
}
