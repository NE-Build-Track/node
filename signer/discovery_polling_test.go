package signer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDiscoveryPollingStartupAndCache(t *testing.T) {
	for name, body := range map[string]string{"empty": "[]", "populated": `[{"address":"https://orch.example","runners":[{"url":"https://runner.example","app":"test","observed_at":"1999-01-01T00:00:00Z"}]}]`} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newService(nil)
				defer s.Close()
				require.NoError(t, s.SetDiscoveryPolling(time.Minute, 5))
				require.NoError(t, s.SetDiscovery([]*url.URL{testURL(t, "https://source.example")}))
				var calls, cancellations, status atomic.Int32
				status.Store(500)
				var gate atomic.Pointer[chan struct{}]
				initial := make(chan struct{})
				gate.Store(&initial)
				s.collector.Client.Transport = webhookTransport(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					require.Empty(t, r.URL.RawQuery)
					if waiting := gate.Load(); waiting != nil {
						select {
						case <-*waiting:
						case <-r.Context().Done():
							cancellations.Add(1)
							return nil, r.Context().Err()
						}
					}
					return &http.Response{StatusCode: int(status.Load()), Body: io.NopCloser(strings.NewReader(body))}, nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var waiters sync.WaitGroup
				requestCtx, cancelRequest := context.WithCancel(t.Context())
				canceled := httptest.NewRecorder()
				waiters.Go(func() {
					s.ServeHTTP(canceled, httptest.NewRequest("GET", "/discover-orchestrators", nil).WithContext(requestCtx))
				})
				synctest.Wait()
				go s.RunDiscovery(ctx)
				for range 8 {
					waiters.Go(func() { require.Equal(t, 503, discoveryResponse(s, "").Code) })
				}
				synctest.Wait()
				cancelRequest()
				synctest.Wait()
				require.Equal(t, 503, canceled.Code)
				require.Zero(t, cancellations.Load(), "canceling the first caller leaves the shared inventory request running")
				require.Equal(t, int32(1), calls.Load(), "startup requests share one inventory fetch")
				close(initial)
				waiters.Wait()
				gate.Store(nil)
				status.Store(200)
				first := discoveryResponse(s, "")
				require.Equal(t, 200, first.Code, "retry lazily before the polling interval")
				require.Equal(t, int32(2), calls.Load())
				stalled := make(chan struct{})
				gate.Store(&stalled)
				time.Sleep(time.Minute)
				synctest.Wait()
				start := time.Now()
				for _, filter := range []string{"", "app=test", "gpu=H100", "app=missing"} {
					require.Equal(t, 200, discoveryResponse(s, filter).Code)
				}
				require.Equal(t, start, time.Now(), "cached requests must not wait for a stalled inventory refresh")
				require.Equal(t, first.Body.String(), discoveryResponse(s, "").Body.String(), "last_seen stays at the successful poll time")
				require.Equal(t, int32(3), calls.Load(), "only the worker polls after initialization")
				status.Store(500)
				close(stalled)
				synctest.Wait()
				time.Sleep(4 * time.Minute)
				synctest.Wait()
				require.Equal(t, 503, discoveryResponse(s, "").Code)
				require.Equal(t, int32(7), calls.Load(), "custom eviction threshold applies and does not restart request fetching")
				shutdown := make(chan struct{})
				gate.Store(&shutdown)
				time.Sleep(time.Minute)
				synctest.Wait()
				cancel()
				synctest.Wait()
				require.Equal(t, int32(1), cancellations.Load(), "shutdown cancels and joins the shared fetch")
			})
		})
	}
}

func TestDiscoveryPollingValidation(t *testing.T) {
	s := newService(nil)
	require.Error(t, s.SetDiscoveryPolling(0, 6))
	require.Error(t, s.SetDiscoveryPolling(time.Minute, -1))
	require.NoError(t, s.SetDiscoveryPolling(time.Minute, 0))
}

func TestDiscoveryPollingOutlivesRequestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newService(nil)
		defer s.Close()
		var sources []*url.URL
		for i := range 42 {
			sources = append(sources, testURL(t, fmt.Sprintf("https://source%d.example", i)))
		}
		require.NoError(t, s.SetDiscovery(sources))
		var calls, cancellations atomic.Int32
		s.collector.Client.Transport = webhookTransport(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			if r.URL.Host != sources[0].Host {
				select {
				case <-time.After(4 * time.Second):
				case <-r.Context().Done():
					cancellations.Add(1)
					return nil, r.Context().Err()
				}
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("[]"))}, nil
		})
		response := httptest.NewRecorder()
		done := make(chan struct{})
		go func() { s.ServeHTTP(response, httptest.NewRequest("GET", "/discover-orchestrators", nil)); close(done) }()
		synctest.Wait() // The request starts the shared fetch before the worker joins.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go s.RunDiscovery(ctx)
		time.Sleep(30 * time.Second)
		<-done
		require.Equal(t, 200, response.Code, "the deadline returns the partial inventory")
		require.Less(t, len(s.collector.Snapshots()), len(sources))
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.Len(t, s.collector.Snapshots(), len(sources), "remaining sources finish without waiting for the next poll")
		require.Equal(t, int32(len(sources)), calls.Load())
		require.Zero(t, cancellations.Load())
	})
}

func TestDiscoveryZeroFailureThreshold(t *testing.T) {
	for _, pollingFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(pollingFirst), func(t *testing.T) {
			s := newService(nil)
			defer s.Close()
			if pollingFirst {
				require.NoError(t, s.SetDiscoveryPolling(time.Minute, 0))
			}
			require.NoError(t, s.SetDiscovery([]*url.URL{testURL(t, "https://source.example")}))
			if !pollingFirst {
				require.NoError(t, s.SetDiscoveryPolling(time.Minute, 0))
			}
			status := 200
			s.collector.Client.Transport = webhookTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("[]"))}, nil
			})
			require.Equal(t, 200, discoveryResponse(s, "").Code)
			status = 500
			s.collector.Refresh(t.Context(), false)
			require.Equal(t, 503, discoveryResponse(s, "").Code, "zero evicts on the first failure")
		})
	}
}

func TestEmptyWebhookEndsInventoryStartupRetries(t *testing.T) {
	s := newService(nil)
	defer s.Close()
	static := []*url.URL{testURL(t, "https://static.example")}
	require.NoError(t, s.SetDiscovery(static))
	require.NoError(t, s.SetOrchestratorWebhook(testURL(t, "https://webhook.example"), nil, time.Minute))
	require.NoError(t, s.SetDiscovery(static))
	body := "[]"
	status := 500
	inventoryCalls := 0
	s.collector.Client.Transport = webhookTransport(func(r *http.Request) (*http.Response, error) {
		data := body
		if r.URL.Host != "webhook.example" {
			inventoryCalls++
			data = "[]"
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(data))}, nil
	})
	require.Equal(t, 503, discoveryResponse(s, "").Code)
	require.Zero(t, inventoryCalls, "an unresolved webhook still overrides static sources after reconfiguration")
	status = 200
	require.Equal(t, 503, discoveryResponse(s, "").Code)
	transport := s.collector.Client.Transport
	require.NoError(t, s.SetDiscovery(static))
	s.collector.Client.Transport = transport
	body = `[{"address":"https://source.example/discovery"}]`
	s.orchWebhook.refresh(t.Context(), s.collector.Client.Transport, false)
	require.Equal(t, 503, discoveryResponse(s, "").Code)
	require.Zero(t, inventoryCalls, "successful empty webhook ends request-triggered startup retries")
	s.collector.Refresh(t.Context(), false)
	require.Equal(t, 200, discoveryResponse(s, "").Code)
	require.Equal(t, 1, inventoryCalls)
}
