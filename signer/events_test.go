package signer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/pm/wire"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type eventSinkFunc func(context.Context, signingEvent) error

func (f eventSinkFunc) Enqueue(ctx context.Context, event signingEvent) error { return f(ctx, event) }

func TestSigningEventContract(t *testing.T) {
	for _, kind := range []string{"live", "fixed"} {
		t.Run(kind, func(t *testing.T) {
			s, info := testService(t)
			info.Address = nil
			key := &failingPaymentSigner{TicketSigner: s.key}
			s.key = key
			proposals := make(chan paymentState, 3)
			var events []signingEvent
			s.events = eventSinkFunc(func(_ context.Context, event signingEvent) error { events = append(events, event); return nil })
			var authCalls int
			webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct{ State paymentState }
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				proposals <- body.State
				if key.calls.Load() != 0 {
					t.Error("payment signed before authorization")
				}
				authCalls++
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "auth_id": "accounting-session", "expiry": time.Now().Add(time.Hour).Unix()})
			}))
			defer webhook.Close()
			require.NoError(t, s.SetAuthWebhook(testURL(t, webhook.URL), nil))
			request := map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": kind, "app": "test-app"}
			var previous paymentState
			for i := range 3 {
				if i == 2 {
					info.Address = info.TicketParams.Recipient
					info.TicketParams.RecipientRandHash = crypto.Keccak256([]byte("refreshed"))
					request["orchestrator"] = wire.EncodeOrchestratorInfo(info)
				}
				response := postPayment(t, s, request)
				require.Equal(t, 200, response.Code, response.Body.String())
				require.Len(t, events, i+1)
				event := events[i]
				id, err := uuid.Parse(event.ID)
				require.NoError(t, err)
				require.Equal(t, byte(4), id[6]>>4, "event IDs retain UUID v4")
				require.Equal(t, byte(2), id[8]>>6, "event IDs retain the RFC variant")
				require.Equal(t, "create_signed_ticket", event.Type)
				require.Empty(t, event.Gateway)
				ts, err := strconv.ParseInt(event.Timestamp, 10, 64)
				require.NoError(t, err)
				require.WithinDuration(t, time.Now(), time.UnixMilli(ts), time.Second)
				var result paymentResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				var state paymentState
				require.NoError(t, json.Unmarshal(result.State.State, &state))
				require.Equal(t, info.TicketParams.Recipient, state.OrchestratorAddress.Bytes())
				encodedPayment, err := base64.StdEncoding.DecodeString(result.Payment)
				require.NoError(t, err)
				payment, err := wire.DecodePayment(encodedPayment)
				require.NoError(t, err)
				data := event.Data
				require.Equal(t, state.StateID, data.SessionID)
				require.Equal(t, "test-app", data.App)
				require.Equal(t, kind, data.Pipeline)
				require.Equal(t, "accounting-session", data.AuthID)
				require.Equal(t, state.FeeWei, data.ComputedFee)
				require.Equal(t, state.FeeUSD, data.ComputedFeeUSD)
				require.NotEmpty(t, data.RequestID)
				require.Equal(t, s.key.Address().Hex(), data.PayerAddress)
				require.Equal(t, state.OrchestratorAddress.Hex(), data.OrchAddress)
				require.Equal(t, info.Transcoder, data.OrchURL)
				require.Equal(t, state.ManifestID, data.ManifestID)
				require.Equal(t, state.PMSessionID, data.PMSessionID)
				require.Equal(t, uint64(i), data.SequenceNumber)
				require.Equal(t, len(payment.PayerParams), data.NumTickets)
				require.Equal(t, state.LastUpdate, data.CurrentTime)
				require.Equal(t, state.LastUpdate.UnixMilli(), data.CurrentTimeUnix)
				require.Equal(t, int64(0), data.Pixels)
				require.Equal(t, "10.0000000000", data.Cost)
				balance, ok := new(big.Rat).SetString(state.Balance)
				require.True(t, ok)
				require.Equal(t, balance.FloatString(0), data.SessionBalance)
				if i == 0 {
					proposed := <-proposals
					approved := state
					approved.AuthID, approved.AuthExpiry = proposed.AuthID, proposed.AuthExpiry
					approved.AuthPolicy, approved.AuthMaxPrice = proposed.AuthPolicy, proposed.AuthMaxPrice
					require.Equal(t, proposed, approved, "approval must preserve all payment fields")
					require.Equal(t, proposed.TicketNonce, payment.PayerParams[len(payment.PayerParams)-1].TicketNonce)
					require.Equal(t, int32(len(payment.PayerParams)+2), key.calls.Load())
					require.Equal(t, "new", data.SessionStatus)
					require.Equal(t, data.CurrentTime, data.PreviousTime)
					fee, seconds := "10", float64(0)
					if kind == "live" {
						fee, seconds = "100", 10
					}
					require.Equal(t, fee, data.ComputedFee)
					require.Equal(t, &fee, data.ComputedFeeUSD)
					require.Equal(t, seconds, data.BillableSecs)
				} else {
					require.Equal(t, "continuing", data.SessionStatus)
					require.Equal(t, previous.LastUpdate, data.PreviousTime)
					require.Equal(t, previous.LastUpdate.UnixMilli(), data.PreviousTimeUnix)
					require.Equal(t, state.LastUpdate.Sub(previous.LastUpdate).Seconds(), data.BillableSecs)
					require.NotEqual(t, events[i-1].ID, event.ID)
					require.NotEqual(t, events[i-1].Data.RequestID, data.RequestID)
				}
				if i == 2 {
					require.NotEqual(t, previous.PMSessionID, data.PMSessionID)
				}
				previous = state
				request["state"] = result.State
			}
			info.TicketParams.Recipient = s.key.Address().Bytes()
			request["orchestrator"] = wire.EncodeOrchestratorInfo(info)
			requirePaymentFailure(t, postPayment(t, s, request), 400)
			require.Len(t, events, 3)
			require.Equal(t, 1, authCalls, "cached authorization still emits every accounting event")
			raw, err := json.Marshal(events[0])
			require.NoError(t, err)
			var envelope map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &envelope))
			require.ElementsMatch(t, []string{"id", "type", "timestamp", "gateway", "data"}, slices.Collect(maps.Keys(envelope)))
			var data map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(envelope["data"], &data))
			require.ElementsMatch(t, []string{"session_id", "session_status", "app", "pipeline", "request_id", "payer_address", "orch_address", "orch_url", "manifest_id", "pm_session_id", "current_time", "current_time_unix", "previous_time", "previous_time_unix", "billable_secs", "pixels", "session_balance", "computed_fee", "computed_fee_usd", "cost", "sequence_number", "num_tickets", "auth_id"}, slices.Collect(maps.Keys(data)))
		})
	}
}

func TestAccountingRateCapturedBeforeAuthorization(t *testing.T) {
	s, info := testService(t)
	require.NoError(t, s.pricePolicy.setRate(big.NewRat(3, 1), time.Time{}))
	var proposal struct{ State paymentState }
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&proposal))
		if s.pricePolicy != nil {
			require.NoError(t, s.pricePolicy.setRate(big.NewRat(6, 1), time.Time{}))
		}
		_, _ = io.WriteString(w, `{"status":200}`)
	}))
	defer webhook.Close()
	require.NoError(t, s.SetAuthWebhook(testURL(t, webhook.URL), nil))
	s.events = eventSinkFunc(func(_ context.Context, event signingEvent) error {
		require.Equal(t, proposal.State.FeeWei, event.Data.ComputedFee)
		require.Equal(t, proposal.State.FeeUSD, event.Data.ComputedFeeUSD)
		return nil
	})
	request := map[string]any{"type": "fixed"}
	for _, want := range []struct {
		price int64
		usd   *string
	}{{10, new("3.333333333333333333")}, {10, new("1.666666666666666667")}, {5, nil}} {
		if want.usd == nil {
			s.pricePolicy = nil
		}
		info.Price.PricePerUnit = want.price
		request["orchestrator"] = wire.EncodeOrchestratorInfo(info)
		response := postPayment(t, s, request)
		require.Equal(t, 200, response.Code, response.Body.String())
		var result paymentResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		var state paymentState
		require.NoError(t, json.Unmarshal(result.State.State, &state))
		require.Equal(t, strconv.FormatInt(want.price, 10), state.FeeWei)
		require.Equal(t, want.usd, state.FeeUSD)
		request["state"] = result.State
	}
}

func TestRejectedPaymentsEmitNoEvent(t *testing.T) {
	for rejection, status := range map[string]int{"invalid": 400, "price": 481, "refresh": 480, "funds": 400} {
		t.Run(rejection, func(t *testing.T) {
			s, info := testService(t)
			s.events = eventSinkFunc(func(context.Context, signingEvent) error { t.Fatal("rejected payment emitted an event"); return nil })
			request := map[string]any{"type": "fixed", "orchestrator": wire.EncodeOrchestratorInfo(info)}
			switch rejection {
			case "invalid":
				request["type"] = "lv2v"
			case "price":
				request["maxPrice"] = map[string]any{"price": "1", "currency": "wei", "unit": "fixed"}
			case "refresh":
				info.Auth.Expiration = time.Now().Unix()
				request["orchestrator"] = wire.EncodeOrchestratorInfo(info)
			case "funds":
				s.SetPaymentChain(unavailablePayer{})
			}
			requirePaymentFailure(t, postPayment(t, s, request), status)
		})
	}
}

type interruptedWriter struct {
	*httptest.ResponseRecorder
	beforeWrite func()
}

func (w interruptedWriter) Write([]byte) (int, error) { w.beforeWrite(); return 0, io.ErrClosedPipe }

func TestEventPersistedBeforeResponseAndSurvivesInterruptedWrite(t *testing.T) {
	s, info := testService(t)
	p := testKafkaProducer(t, 1<<20)
	s.events = p
	w := interruptedWriter{httptest.NewRecorder(), func() {
		events, err := p.outbox.pending(t.Context())
		require.NoError(t, err)
		require.Len(t, events, 1, "event must be durable before attempting the response write")
	}}
	body, err := json.Marshal(map[string]any{"type": "fixed", "orchestrator": wire.EncodeOrchestratorInfo(info)})
	require.NoError(t, err)
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/generate-live-payment", bytes.NewReader(body)))
	events, err := p.outbox.pending(t.Context())
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Empty(t, w.Body.String())
	// Storage failure must withhold payment, credentials, and signed state.
	require.NoError(t, p.outbox.Close())
	response := postPayment(t, s, map[string]any{"type": "fixed", "orchestrator": wire.EncodeOrchestratorInfo(info)})
	requirePaymentFailure(t, response, 503)
	require.Equal(t, uint64(1), p.enqueueFailures.Load())
}

func TestOversizedEventsWithholdPaymentWithoutBlockingDelivery(t *testing.T) {
	for name, quota := range map[string]int64{"outbox": 2048, "Kafka": 256 << 20} {
		t.Run(name, func(t *testing.T) {
			s, info := testService(t)
			p := testKafkaProducer(t, quota)
			s.events = p
			app := strings.Repeat("a", 3000)
			if name == "Kafka" {
				app, info.Transcoder = "", "https://orch.example.com/?q="+strings.Repeat("<", 200000)
			}
			response := postPayment(t, s, map[string]any{"type": "fixed", "app": app, "orchestrator": wire.EncodeOrchestratorInfo(info)})
			require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
			require.NotContains(t, response.Body.String(), "\"payment\"")
			stats, err := p.outbox.stats(t.Context())
			require.NoError(t, err)
			require.Zero(t, stats.Count)
			require.True(t, p.ready(t.Context()))
			require.Zero(t, p.storageFailures.Load())
			info.Transcoder = "https://orch.example.com"
			require.Equal(t, 200, postPayment(t, s, map[string]any{"type": "fixed", "orchestrator": wire.EncodeOrchestratorInfo(info)}).Code)
			p.writer = writerFunc(func(_ context.Context, messages ...kafka.Message) error { require.Len(t, messages, 1); return nil })
			progress, err := p.publish(t.Context())
			require.NoError(t, err)
			require.True(t, progress)
		})
	}
}
