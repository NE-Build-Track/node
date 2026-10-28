package orchestrator

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

func TestRunnerCapacityOutcomes(t *testing.T) {
	for _, paid := range []bool{false, true} {
		name := "unpaid"
		if paid {
			name = "paid"
		}
		t.Run(name, func(t *testing.T) {
			registry, app := reviewServer(t, "http://127.0.0.1:1")
			defer app.Close()
			registry.proxyTemplate = "https://{proxy}.apps.example"
			if paid {
				registry.SetWeiPerUSD(big.NewRat(1, 1))
			}
			require.NoError(t, registry.AddStatic(StaticRunner{ID: "runner", App: "model", Mode: "single-shot", RunnerURL: "http://127.0.0.1:1", Capacity: 1, Proxy: true, PriceInfo: priceInfo{Price: "1", Currency: "usd", Unit: "fixed"}}))
			_, _, _, _, err := registry.ReserveWithID("runner", "held")
			require.NoError(t, err)
			if paid {
				store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
				require.NoError(t, err)
				defer store.Close()
				key := paymentTestKey(t)
				engine, err := NewPaymentEngine(store, paymentTestChain{}, key.Address(), big.NewInt(11), big.NewInt(1))
				require.NoError(t, err)
				app.SetPayment(engine)
				_, err = engine.MakeChallenge(t.Context(), "runner", "new", key.Address(), 1, "fixed", registry.service)
				require.NoError(t, err)
			}
			for _, route := range []string{"/apps/runner/app", "/run/runner", "/proxy/runner", "https://runner.apps.example/"} {
				request := httptest.NewRequest("GET", route, nil)
				if paid {
					request.Header.Set("Livepeer-Payment", "pending")
					request.Header.Set("Livepeer-Segment", base64.StdEncoding.EncodeToString(wire.EncodeSegData(wire.SegData{ManifestID: []byte("new"), Auth: wire.AuthToken{SessionID: "new"}})))
				}
				response := httptest.NewRecorder()
				app.ServeHTTP(response, request)
				require.Equal(t, 503, response.Code, response.Body.String())
			}
			require.EqualValues(t, 4, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"outcome": "capacity"}))
			require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"outcome": "error"}))
			require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "upstream_requests_total", nil))
		})
	}
}

func TestUpgradePreservesHalfClose(t *testing.T) {
	for _, clientFirst := range []bool{true, false} {
		name := "backend_first"
		if clientFirst {
			name = "client_first"
		}
		t.Run(name, func(t *testing.T) {
			backendReply := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, rw, err := w.(http.Hijacker).Hijack()
				require.NoError(t, err)
				defer conn.Close()
				require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
				_, err = io.WriteString(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: custom\r\n\r\n")
				require.NoError(t, err)
				require.NoError(t, rw.Flush())
				if clientFirst {
					body, err := io.ReadAll(rw)
					require.NoError(t, err)
					_, err = io.WriteString(conn, "reply:"+string(body))
					require.NoError(t, err)
				} else {
					_, err = io.WriteString(conn, "payload")
					require.NoError(t, err)
					require.NoError(t, conn.(interface{ CloseWrite() error }).CloseWrite())
					body, err := io.ReadAll(rw)
					require.NoError(t, err)
					backendReply <- string(body)
				}
			}))
			defer upstream.Close()
			registry, app := reviewServer(t, upstream.URL)
			defer app.Close()
			require.NoError(t, registry.AddStatic(StaticRunner{ID: "runner", App: "model", Mode: "single-shot", RunnerURL: upstream.URL, Capacity: 1}))
			server := httptest.NewServer(app)
			defer server.Close()
			u, err := url.Parse(server.URL)
			require.NoError(t, err)
			address, err := net.ResolveTCPAddr("tcp", u.Host)
			require.NoError(t, err)
			conn, err := net.DialTCP("tcp", nil, address)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(4*time.Second)))
			_, err = io.WriteString(conn, "GET /apps/runner/app HTTP/1.1\r\nHost: "+u.Host+"\r\nConnection: Upgrade\r\nUpgrade: custom\r\n\r\n")
			require.NoError(t, err)
			reader := bufio.NewReader(conn)
			response, err := http.ReadResponse(reader, &http.Request{Method: "GET"})
			require.NoError(t, err)
			require.Equal(t, 101, response.StatusCode)
			if clientFirst {
				_, err = io.WriteString(conn, "payload")
				require.NoError(t, err)
				require.NoError(t, conn.CloseWrite())
				body, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, "reply:payload", string(body))
			} else {
				body, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, "payload", string(body))
				_, err = io.WriteString(conn, "reply:"+string(body))
				require.NoError(t, err)
				require.NoError(t, conn.CloseWrite())
				select {
				case reply := <-backendReply:
					require.Equal(t, "reply:payload", reply)
				case <-time.After(4 * time.Second):
					t.Fatal("backend did not receive the client reply after half-close")
				}
			}
		})
	}
}

func TestApplicationMetricsAndInventory(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer upstream.Close()
	registry, app := reviewServer(t, upstream.URL)
	defer app.Close()
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "runner", App: "model", Mode: "single-shot", RunnerURL: upstream.URL, Capacity: 1, Proxy: true}))
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "runner_registered", map[string]string{"app": "model"}))
	for _, state := range []string{"pending", "active"} {
		samples := test.MetricSamples(t, registry.metrics.Registry, "runner_sessions", map[string]string{"runner_id": "runner", "state": state})
		require.Len(t, samples, 1)
		require.Zero(t, samples[0].Gauge.GetValue())
	}
	for _, path := range []string{"/apps/runner/app", "/run/runner", "/proxy/runner"} {
		response := httptest.NewRecorder()
		app.ServeHTTP(response, httptest.NewRequest("POST", path, nil))
		require.Equal(t, 503, response.Code)
	}
	require.EqualValues(t, 3, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"app": "model", "outcome": "error"}))
	require.EqualValues(t, 3, test.MetricSum(t, registry.metrics.Registry, "upstream_requests_total", map[string]string{"outcome": "http_5xx"}))
	require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "runner_sessions", nil))
	// Admission rejection is still an application call, but never an upstream attempt.
	for range cap(app.streamSlots) {
		app.streamSlots <- struct{}{}
	}
	app.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/run/runner", nil))
	for range cap(app.streamSlots) {
		<-app.streamSlots
	}
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"outcome": "capacity"}))
	require.EqualValues(t, 3, test.MetricSum(t, registry.metrics.Registry, "upstream_requests_total", nil))
	// An application change affects new sessions, never an existing identity.
	registry.mu.Lock()
	registry.runners["runner"].Mode = "persistent"
	registry.mu.Unlock()
	sid, _, _, _, err := registry.Reserve("runner")
	require.NoError(t, err)
	registry.mu.Lock()
	registry.runners["runner"].App = "new-model"
	registry.mu.Unlock()
	require.Equal(t, "model", registry.identity("runner", sid).App)
	registry.ReleaseBySession(sid)
	require.EqualValues(t, 4, test.MetricSum(t, registry.metrics.Registry, "session_ends_total", map[string]string{"app": "model"}))
}

func TestPaidDependencyObservationsRecover(t *testing.T) {
	f, store, ticket, chain, key := newRecoveryTest(t, 15)
	metrics := observability.New("orchestrator")
	registry := NewRegistry("", "http://localhost", time.Second, time.Minute)
	p := &paidReadiness{interval: time.Second, workerCompleted: time.Now(), account: key.Address()}
	var events bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&events, nil)).With("component", "orchestrator")
	check := func() {
		p.check(t.Context(), store, chain, &PaymentEngine{recipient: ticket.Recipient}, metrics, logger)
	}
	f.mu.Lock()
	f.snapshotError = true
	f.mu.Unlock()
	_, err := store.db.Exec("PRAGMA query_only=ON")
	require.NoError(t, err)
	check()
	var event map[string]any
	require.NoError(t, json.Unmarshal(events.Bytes(), &event))
	for key, value := range map[string]string{"component": "orchestrator", "event": "payment_dependencies", "operation": "check", "outcome": "unavailable"} {
		require.Equal(t, value, event[key])
	}
	first := events.String()
	check()
	require.Equal(t, first, events.String(), "unchanged dependencies do not repeat the event")
	f.mu.Lock()
	f.snapshotError = false
	f.mu.Unlock()
	_, err = store.db.Exec("PRAGMA query_only=OFF")
	require.NoError(t, err)
	check()
	require.True(t, p.ready(registry))
	var enqueued string
	require.NoError(t, store.db.QueryRow("SELECT enqueued_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&enqueued))
	_, err = store.db.Exec("PRAGMA query_only=ON")
	require.NoError(t, err)
	check()
	require.False(t, p.storage)
	require.False(t, p.ready(registry))
	require.Zero(t, test.MetricSum(t, metrics.Registry, "dependency_healthy", map[string]string{"dependency": "payment_storage"}))
	_, err = store.db.Exec("PRAGMA query_only=OFF")
	require.NoError(t, err)
	check()
	require.True(t, p.storage)
	require.True(t, p.ready(registry))
	var after string
	require.NoError(t, store.db.QueryRow("SELECT enqueued_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&after))
	require.Equal(t, enqueued, after, "health observations must not change accounting state")
	for _, failure := range []string{"snapshot", "active", "worker", "observation", "price"} {
		t.Run(failure, func(t *testing.T) {
			f.mu.Lock()
			f.snapshotError, f.activeError = failure == "snapshot", failure == "active"
			f.mu.Unlock()
			if failure == "worker" {
				p.workerCompleted = time.Now().Add(-time.Hour)
			}
			check()
			if failure == "observation" {
				p.completed = time.Now().Add(-time.Hour)
			}
			if failure == "price" {
				registry.rateUntil = time.Now().Add(-time.Second)
			}
			require.False(t, p.ready(registry))
			f.mu.Lock()
			f.snapshotError, f.activeError = false, false
			f.mu.Unlock()
			p.workerCompleted, registry.rateUntil = time.Now(), time.Time{}
			check()
			require.True(t, p.ready(registry))
		})
	}
}

func TestUpstreamFailureAfterHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("short response"))
	}))
	defer upstream.Close()
	registry, app := reviewServer(t, upstream.URL)
	defer app.Close()
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "runner", App: "model", Mode: "single-shot", RunnerURL: upstream.URL, Capacity: 1}))
	response := httptest.NewRecorder()
	app.ServeHTTP(response, httptest.NewRequest("GET", "/apps/runner/app", nil))
	require.Equal(t, 200, response.Code, "the upstream status was already forwarded")
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"outcome": "stream_error"}))
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "stream_errors_total", nil))
	require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "app_calls_in_flight", nil))
}

func TestRedemptionMetricsDurableAndIdempotent(t *testing.T) {
	_, store, ticket, _, _ := newRecoveryTest(t, 14)
	store.metrics = observability.New("orchestrator")
	store.metrics.Snapshot(store.collectMetrics)
	store.readiness = &paidReadiness{completed: time.Now(), chain: true, interval: time.Second, snapshot: eth.ChainSnapshot{Block: new(big.Int).Sub(ticket.ParamsExpirationBlock, big.NewInt(1)), Round: big.NewInt(ticket.CreationRound)}}
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_queue_tickets", map[string]string{"state": "waiting"}))
	var enqueued string
	require.NoError(t, store.db.QueryRow("SELECT enqueued_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&enqueued))
	store.readiness.snapshot.Block = new(big.Int).Set(ticket.ParamsExpirationBlock)
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_queue_tickets", map[string]string{"state": "eligible"}))
	_, err := store.db.Exec("UPDATE winning_tickets SET params_expiration_block='broken' WHERE sig=?", ticket.Sig)
	require.NoError(t, err)
	values := map[string]float64{}
	store.collectMetrics(func(name string, value float64, _ observability.Labels) { values[name] = value })
	require.Equal(t, map[string]float64{"collector_healthy": 0}, values, "collection failure must expose health and omit unavailable statistics")
	_, err = store.db.Exec("UPDATE winning_tickets SET params_expiration_block=? WHERE sig=?", ticket.ParamsExpirationBlock.String(), ticket.Sig)
	require.NoError(t, err)
	require.NoError(t, store.setTicketPhase(t.Context(), ticket.Sig, "awaiting_finality"))
	require.NoError(t, store.setTicketPhase(t.Context(), ticket.Sig, "awaiting_finality"))
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "awaiting_finality"}))
	require.NoError(t, store.setTicketPhase(t.Context(), ticket.Sig, "retryable"))
	require.NoError(t, store.setTicketPhase(t.Context(), ticket.Sig, "retryable"))
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	_, err = store.db.Exec(`CREATE TRIGGER fail_transition BEFORE UPDATE OF phase ON redemption_attempts WHEN NEW.phase='awaiting_finality' BEGIN SELECT RAISE(ABORT,'storage failure'); END`)
	require.NoError(t, err)
	require.Error(t, store.setTicketPhase(t.Context(), ticket.Sig, "awaiting_finality"))
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "awaiting_finality"}))
	_, err = store.db.Exec(`DROP TRIGGER fail_transition`)
	require.NoError(t, err)
	require.NoError(t, store.finishTicket(t.Context(), ticket.Sig, "used", ""))
	require.NoError(t, store.finishTicket(t.Context(), ticket.Sig, "used", ""))
	require.NoError(t, store.finishTicket(t.Context(), ticket.Sig, "confirmed", ""))
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_settlements_total", nil))
	require.Zero(t, test.MetricSum(t, store.metrics.Registry, "redemption_queue_tickets", nil))
	store = reopenRecoveryStore(t, store)
	var restored string
	require.NoError(t, store.db.QueryRow("SELECT enqueued_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&restored))
	require.Equal(t, enqueued, restored)
	require.NoError(t, store.Close())
	store.metrics = observability.New("orchestrator")
	store.metrics.Snapshot(store.collectMetrics)
	require.Zero(t, test.MetricSum(t, store.metrics.Registry, "collector_healthy", nil))
	require.Zero(t, test.MetricSum(t, store.metrics.Registry, "redemption_queue_tickets", nil))
}
