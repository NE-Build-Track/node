package signer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	testhelpers "github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

func TestAuthWebhookCachePriceAndIdentity(t *testing.T) {
	s, info := testService(t)
	countingKey := &failingPaymentSigner{TicketSigner: s.key}
	s.key = countingKey
	var calls atomic.Int32
	var reject atomic.Bool
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "configured", r.Header.Get("X-Webhook-Secret"))
		status := 200
		if reject.Load() {
			status = 403
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "reason": "denied", "expiry": time.Now().Add(time.Minute).Unix(), "auth_id": "alice", "maxPrice": map[string]any{"price": "10", "currency": " WEI ", "unit": " FIXED "}})
	}))
	defer webhook.Close()
	require.NoError(t, s.SetAuthWebhook(testURL(t, webhook.URL), Headers{"X-Webhook-Secret": {"configured"}}))
	req := map[string]any{"orchestrator": base64.StdEncoding.EncodeToString(wire.EncodeOrchestratorInfo(info)), "type": "fixed"}
	w := postPayment(t, s, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	var first paymentResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &first))
	var state paymentState
	require.NoError(t, json.Unmarshal(first.State.State, &state))
	require.Equal(t, "alice", state.AuthID)
	require.Equal(t, "10", state.AuthMaxPrice)
	req["state"] = first.State
	w = postPayment(t, s, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, int32(1), calls.Load())
	// Inactive recipients have zero claimable reserve, even with a funded deposit.
	s.SetPaymentChain(payerChainFunc(func(ctx context.Context, payer, recipient ethcommon.Address) (pm.PayerFunds, error) {
		require.Equal(t, ethcommon.BytesToAddress(info.TicketParams.Recipient), recipient)
		funds, err := (fundedPayer{}).PayerFunds(ctx, payer, recipient)
		funds.Reserve.SetInt64(0)
		return funds, err
	}))
	beforeSignatures := countingKey.calls.Load()
	requirePaymentFailure(t, postPayment(t, s, req), 400)
	require.Equal(t, beforeSignatures, countingKey.calls.Load(), "zero claimable reserve must prevent signing despite cached approval")
	require.Equal(t, int32(1), calls.Load())
	s.SetPaymentChain(fundedPayer{})
	state.AuthExpiry = time.Now().Add(-time.Minute).Unix()
	expired := httptest.NewRequest("POST", "/generate-live-payment", nil)
	expired.Header.Set("Signer-Auth-Id", "bob")
	require.NoError(t, s.authorizePayment(expired, paymentRequest{Type: "fixed"}, info.Price, &state))
	require.Equal(t, "alice", state.AuthID, "webhook identity takes precedence over the request header")
	require.Equal(t, int32(2), calls.Load(), "expired authorization must call the webhook again")
	// Identity checks apply to signed state on any replica.
	body, err := json.Marshal(req)
	require.NoError(t, err)
	r := httptest.NewRequest("POST", "/generate-live-payment", strings.NewReader(string(body)))
	r.Header.Set("Signer-Auth-Id", "bob")
	w = httptest.NewRecorder()
	beforeSignatures = countingKey.calls.Load()
	s.ServeHTTP(w, r)
	require.Equal(t, 403, w.Code)
	require.Equal(t, beforeSignatures, countingKey.calls.Load(), "cached identity rejection must not sign")
	// Changing configured credentials invalidates prior webhook authorization.
	var second paymentResponse
	require.NoError(t, json.Unmarshal(postPayment(t, s, req).Body.Bytes(), &second))
	req["state"] = second.State
	reject.Store(true)
	require.NoError(t, s.SetAuthWebhook(testURL(t, webhook.URL), Headers{"X-Webhook-Secret": {"configured"}, "X-Policy": {"new"}}))
	beforeSignatures = countingKey.calls.Load()
	w = postPayment(t, s, req)
	require.Equal(t, 403, w.Code, w.Body.String())
	require.Equal(t, beforeSignatures, countingKey.calls.Load())
	require.Equal(t, int32(3), calls.Load())
	require.NotContains(t, w.Body.String(), `"payment"`)
	require.NotContains(t, w.Body.String(), `"state"`)
	// An approving webhook still cannot replace an established identity.
	reject.Store(false)
	conflicting := state
	conflicting.AuthID, conflicting.AuthExpiry = "bob", 0
	var mismatch paymentFailure
	require.ErrorAs(t, s.authorizePayment(httptest.NewRequest("POST", "/generate-live-payment", nil), paymentRequest{Type: "fixed"}, info.Price, &conflicting), &mismatch)
	require.Equal(t, 403, mismatch.status)
}

func TestAuthWebhookFailuresWithholdPayment(t *testing.T) {
	logs := captureSignerLogs(t)
	tests := map[string]struct {
		body         string
		status, want int
	}{
		"/http-error": {"private-webhook-detail", 500, 502}, "/redirect": {"", 302, 502},
		"/malformed": {"not JSON: private-webhook-detail", 200, 502}, "/oversized": {strings.Repeat(" ", (1<<20)+1), 200, 502},
		"/missing-status": {`{}`, 200, 502}, "/invalid-status": {`{"status":201}`, 200, 502},
		"/invalid-reason":   {`{"status":403,"reason":123}`, 200, 502},
		"/zero-price":       {`{"status":200,"maxPrice":{"price":0,"currency":"wei","unit":"fixed"}}`, 200, 502},
		"/missing-price":    {`{"status":200,"maxPrice":{"currency":"wei","unit":"fixed"}}`, 200, 502},
		"/invalid-currency": {`{"status":200,"maxPrice":{"price":10,"currency":"usd","unit":"fixed"}}`, 200, 502},
		"/invalid-unit":     {`{"status":200,"maxPrice":{"price":10,"currency":"wei","unit":"seconds"}}`, 200, 502},
		"/price-limit":      {`{"status":200,"maxPrice":{"price":9,"currency":"wei","unit":"fixed"}}`, 200, 481},
		"/insufficient":     {`{"status":402}`, 200, 402},
		"/denied":           {`{"status":403}`, 200, 403},
		"/private-reason":   {`{"status":403,"reason":"Bearer private-webhook-detail: SQL authorization failed"}`, 200, 403},
		"/private-failure":  {`{"status":502,"reason":"https://auth.internal/?token=private-webhook-detail"}`, 200, 502},
	}
	var calls atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		test := tests[r.URL.Path]
		w.Header().Set("Location", "/redirect")
		w.WriteHeader(test.status)
		_, _ = io.WriteString(w, test.body)
	}))
	defer webhook.Close()
	s, info := testService(t)
	defer s.Close()
	countingKey := &failingPaymentSigner{TicketSigner: s.key}
	s.key = countingKey
	s.events = eventSinkFunc(func(context.Context, signingEvent) error {
		t.Fatal("authorization failure emitted an event")
		return nil
	})
	request := map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"}
	for path, test := range tests {
		t.Run(path, func(t *testing.T) {
			logs.Reset()
			before := calls.Load()
			require.NoError(t, s.SetAuthWebhook(testURL(t, webhook.URL+path), nil))
			outcome := "error"
			switch path {
			case "/price-limit", "/insufficient", "/denied", "/private-reason", "/private-failure":
				outcome = "rejected"
			}
			beforeOutcome := testhelpers.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": outcome})
			webhookOutcome := "error"
			if outcome == "rejected" {
				webhookOutcome = "denied"
			}
			beforeWebhook := testhelpers.MetricSum(t, s.metrics.Registry, "signer_auth_webhook_requests_total", map[string]string{"outcome": webhookOutcome})
			response := postPayment(t, s, request)
			requirePaymentFailure(t, response, test.want)
			require.Equal(t, beforeOutcome+1, testhelpers.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": outcome}))
			require.Equal(t, beforeWebhook+1, testhelpers.MetricSum(t, s.metrics.Registry, "signer_auth_webhook_requests_total", map[string]string{"outcome": webhookOutcome}))
			require.Zero(t, countingKey.calls.Load(), "failed authorization must not sign tickets, segment credentials, or state")
			require.NotContains(t, response.Body.String(), "private-webhook-detail")
			require.NotEmpty(t, logs.String())
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var fields map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &fields))
				for _, key := range []string{"component", "event", "operation", "outcome"} {
					require.NotEmpty(t, fields[key], key)
				}
			}
			require.NotContains(t, logs.String(), "private-webhook-detail")
			if path == "/http-error" || path == "/redirect" {
				require.Contains(t, logs.String(), `"http_status":`+strconv.Itoa(test.status))
			}
			if path == "/malformed" {
				require.Contains(t, logs.String(), "invalid_json")
			}
			if path == "/invalid-reason" {
				require.Contains(t, logs.String(), "invalid_json_type")
			}
			if path == "/price-limit" {
				require.Contains(t, logs.String(), `"outcome":"denied"`)
				require.NotContains(t, logs.String(), `"outcome":"error"`)
			}
			if strings.HasPrefix(path, "/private-") || path == "/denied" {
				require.Contains(t, response.Body.String(), "signer auth rejected request with status "+strconv.Itoa(test.want))
			}
			if test.status == 302 {
				require.Equal(t, before+1, calls.Load(), "authorization redirects must not be followed")
			}
		})
	}
	webhook.Close()
	logs.Reset()
	require.NoError(t, s.SetAuthWebhook(testURL(t, webhook.URL+"?token=private-webhook-detail"), nil))
	response := postPayment(t, s, request)
	requirePaymentFailure(t, response, 502)
	require.Zero(t, countingKey.calls.Load())
	require.NotContains(t, response.Body.String(), "private-webhook-detail")
	require.Contains(t, logs.String(), "signer auth webhook unavailable")
	require.NotContains(t, logs.String(), "private-webhook-detail")
	require.Contains(t, logs.String(), "connection_refused")
	require.Zero(t, testhelpers.MetricSum(t, s.metrics.Registry, "signer_sign_operations_total", nil))
	require.Zero(t, testhelpers.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "success"}))
	require.EqualValues(t, len(tests)+1, testhelpers.MetricSum(t, s.metrics.Registry, "signer_auth_webhook_requests_total", nil))
}
