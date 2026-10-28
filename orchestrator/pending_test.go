package orchestrator

import (
	"math/big"
	"testing"
	"time"

	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestPendingPaymentDoesNotExposeOrBillSession(t *testing.T) {
	r := NewRegistry("", "https://orch.example", time.Second, time.Minute)
	r.SetWeiPerUSD(big.NewRat(10, 1))
	require.NoError(t, r.AddStatic(StaticRunner{ID: "r", RunnerURL: "https://runner.example", App: "app", Proxy: true, PriceInfo: priceInfo{Price: "1", Currency: "usd", Unit: "fixed"}}))
	quote, _, err := r.PriceForRunner("r")
	require.NoError(t, err)
	var events []string
	var reservedToken string
	r.onEvent = func(_, event, _, token string) {
		events = append(events, event)
		reservedToken = token
	}
	id, _, _, _, err := r.reserveWithPrice("r", "payment-manifest", &quote)
	require.NoError(t, err)
	require.Empty(t, events)
	require.Empty(t, r.PaidSessions())
	_, _, status, err := r.sessionTarget("r", id)
	require.Error(t, err)
	require.Equal(t, 404, status)
	for proxy := range r.runners["r"].Sessions[id].Proxies {
		_, _, _, _, ok := r.proxyTarget(proxy)
		require.False(t, ok)
	}
	require.True(t, r.activateSession("r", id))
	require.Equal(t, []string{"reserved"}, events)
	require.Equal(t, []string{id}, r.PaidSessions())
	_, token, status, err := r.sessionTarget("r", id)
	require.NoError(t, err)
	require.Equal(t, 200, status)
	require.NotEmpty(t, reservedToken)
	require.Equal(t, token, reservedToken)
	r.ReleaseBySession(id)
	require.False(t, r.activateSession("r", id), "a released pending reservation cannot be resurrected")
	for _, reason := range []string{"shutdown", "runner_expired", "runner_unregistered", "runner_unhealthy", "completed", "released", ""} {
		id, _, _, _, err := r.reserveWithPrice("r", "pending-"+reason, &quote)
		require.NoError(t, err)
		want := reason
		if reason == "completed" || reason == "released" || reason == "" {
			want = "reservation_failed"
		}
		before := test.MetricSum(t, r.metrics.Registry, "session_ends_total", map[string]string{"reason": want})
		if reason == "" {
			r.ReleaseBySession(id)
		} else {
			r.ReleaseBySession(id, reason)
		}
		require.Equal(t, before+1, test.MetricSum(t, r.metrics.Registry, "session_ends_total", map[string]string{"reason": want}))
	}
	require.EqualValues(t, 1, test.MetricSum(t, r.metrics.Registry, "session_duration_seconds", nil), "only the activated session contributes duration")

}
