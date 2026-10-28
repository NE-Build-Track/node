package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/livepeer/node/signer"
	"github.com/stretchr/testify/require"
)

type paymentTestChain struct{}

func (paymentTestChain) Snapshot(context.Context) (pm.ChainSnapshot, error) {
	return pm.ChainSnapshot{Block: big.NewInt(50), Round: big.NewInt(5), RoundHash: ethcommon.HexToHash("0x1234")}, nil
}
func (c paymentTestChain) PayerFunds(ctx context.Context, _, _ ethcommon.Address) (pm.PayerFunds, error) {
	snapshot, err := c.Snapshot(ctx)
	return pm.PayerFunds{Snapshot: snapshot, Deposit: big.NewInt(1000000000), Reserve: big.NewInt(1000000000), WithdrawRound: new(big.Int)}, err
}
func (paymentTestChain) IsActiveAt(context.Context, ethcommon.Address, pm.ChainSnapshot) (bool, error) {
	return true, nil
}

func paymentTestKey(t *testing.T) *eth.Key {
	t.Helper()
	path, passwordPath := test.WriteKeystore(t, nil)
	key, err := eth.OpenKeystoreFile(path, passwordPath)
	require.NoError(t, err)
	return key
}

// Adapted from go-livepeer/server/ai_http_test.go's paid reservation,
// fixed-price accounting, and follow-up payment rejection cases.
func TestPaidFixedSessionWithRemoteSigner(t *testing.T) {
	recipient := paymentTestKey(t)
	payer := paymentTestKey(t)
	store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "recipient.sqlite"))
	require.NoError(t, err)
	defer store.Close()
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	engine, err := NewPaymentEngine(store, paymentTestChain{}, recipient.Address(), big.NewInt(11), new(big.Int).Sub(max, big.NewInt(1)))
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://127.0.0.1:8935", time.Second, 30*time.Second)
	registry.SetWeiPerUSD(big.NewRat(10, 1))
	resp, status, err := registry.Heartbeat(heartbeatRequest{RunnerID: "runner1", RunnerURL: "http://127.0.0.1:9000", App: "demo", Mode: "persistent", Capacity: 1, PriceInfo: priceInfo{Price: "1", Currency: "usd", Unit: "fixed"}}, "bootstrap")
	require.NoError(t, err)
	require.Equal(t, 200, status)
	require.Equal(t, "runner1", resp.RunnerID)
	policy, err := destination.New("runner", []string{"127.0.0.1:9000"})
	require.NoError(t, err)
	server := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server.SetPayment(engine)
	signerService, err := signer.NewService(payer)
	require.NoError(t, err)
	defer signerService.Close()
	signerService.SetPaymentChain(paymentTestChain{})
	reserveURL := "/apps/runner1/session"
	first := httptest.NewRequest(http.MethodPost, reserveURL, nil)
	first.Header.Set("Livepeer-Payer-Address", payer.Address().Hex())
	w := httptest.NewRecorder()
	server.ServeHTTP(w, first)
	require.Equal(t, 402, w.Code, w.Body.String())
	var challenge Challenge
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &challenge))
	require.NotEmpty(t, challenge.PaymentParams)
	signerBody, err := json.Marshal(map[string]any{"orchestrator": challenge.PaymentParams, "type": "fixed", "ManifestID": challenge.ManifestID})
	require.NoError(t, err)
	signReq := httptest.NewRequest(http.MethodPost, "/generate-live-payment", bytes.NewReader(signerBody))
	signed := httptest.NewRecorder()
	signerService.ServeHTTP(signed, signReq)
	require.Equal(t, 200, signed.Code, signed.Body.String())
	var payment struct {
		Payment  string `json:"payment"`
		SegCreds string `json:"segCreds"`
	}
	require.NoError(t, json.Unmarshal(signed.Body.Bytes(), &payment))
	// A quote already issued to the payer remains valid when a heartbeat
	// changes the advertised price before the reservation is paid.
	_, _, err = registry.Heartbeat(heartbeatRequest{RunnerID: "runner1", RunnerURL: "http://127.0.0.1:9000", App: "demo", Mode: "persistent", Capacity: 1, PriceInfo: priceInfo{Price: "2", Currency: "usd", Unit: "fixed"}}, resp.HeartbeatSecret)
	require.NoError(t, err)
	paid := httptest.NewRequest(http.MethodPost, reserveURL, nil)
	paid.Header.Set("Livepeer-Payment", payment.Payment)
	paid.Header.Set("Livepeer-Segment", payment.SegCreds)
	occupied, _, _, _, err := registry.Reserve("runner1")
	require.NoError(t, err)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, paid.Clone(t.Context()))
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	count, err := store.WinningTicketCount(payer.Address(), 0)
	require.NoError(t, err)
	require.Zero(t, count, "capacity rejection must not accept the ticket")
	registry.ReleaseBySession(occupied)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, paid)
	require.Equal(t, 200, w.Code, w.Body.String())
	var reservation map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reservation))
	require.Equal(t, challenge.ManifestID, reservation["session_id"])
	pinned, _, err := registry.PriceForSession("runner1", challenge.ManifestID)
	require.NoError(t, err)
	require.Equal(t, "10", pinned.Price.String())
	balance, err := engine.Balance(challenge.ManifestID)
	require.NoError(t, err)
	require.True(t, balance.Sign() >= 0 && balance.Cmp(big.NewRat(1, 1)) < 0)
	count, err = store.WinningTicketCount(payer.Address(), 0)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	w = httptest.NewRecorder()
	followup := httptest.NewRequest(http.MethodPost, challenge.PaymentURL, nil)
	server.ServeHTTP(w, followup)
	require.Equal(t, 409, w.Code)
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "payment_tickets_total", map[string]string{"kind": "accepted"}))
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "payment_fixed_charges_total", map[string]string{"app": "demo"}))
	for range 3 {
		server.ChargePaidSessions(t.Context())
	}
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "payment_operations_total", map[string]string{"operation": "charge"}), "scheduler ticks must not count another fixed-price debit")
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "payment_operations_total", map[string]string{"operation": "receive", "outcome": "success"}))
}

func TestPinnedPythonPaidSingleShot(t *testing.T) {
	if testing.Short() {
		t.Skip("Python SDK integration test")
	}
	python, export, root := pinnedPythonSDK(t)

	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Livepeer-Session-Token") == "" {
			http.Error(w, "missing session", http.StatusForbidden)
			return
		}
		var request map[string]string
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"paid": true, "prompt": request["prompt"]})
	}))
	defer runner.Close()
	payer := paymentTestKey(t)
	recipient := paymentTestKey(t)
	store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "recipient.sqlite"))
	require.NoError(t, err)
	defer store.Close()
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	engine, err := NewPaymentEngine(store, paymentTestChain{}, recipient.Address(), big.NewInt(11), new(big.Int).Sub(max, big.NewInt(1)))
	require.NoError(t, err)
	registry := NewRegistry("", "http://127.0.0.1:0", time.Second, 30*time.Second)
	registry.SetWeiPerUSD(big.NewRat(10, 1))
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "paid-python", RunnerURL: runner.URL, App: "paid-python-app", Mode: "single-shot", Status: "ready", Capacity: 1, PriceInfo: priceInfo{Price: "1", Currency: "usd", Unit: "fixed"}}))
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "paid-python-live", RunnerURL: runner.URL, App: "paid-python-app", Mode: "single-shot", Status: "ready", Capacity: 1, PriceInfo: priceInfo{Price: "3600", Currency: "usd", Unit: "hour"}}))
	policy, err := destination.New("runner", []string{strings.TrimPrefix(runner.URL, "http://")})
	require.NoError(t, err)
	server := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server.SetPayment(engine)
	orch := httptest.NewUnstartedServer(server)
	defer orch.Close()
	registry.service = "http://" + orch.Listener.Addr().String()
	orch.Start()
	signerService, err := signer.NewService(payer)
	require.NoError(t, err)
	defer signerService.Close()
	signerService.SetPaymentChain(paymentTestChain{})
	signerHTTP := httptest.NewServer(signerService)
	defer signerHTTP.Close()
	fixture := filepath.Join(root, "cmd", "livepeer", "testdata", "python_paid_compat.py")
	command := exec.Command(python, fixture, orch.URL, signerHTTP.URL)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(export, "src"))
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Pinned Python runner paid fixed and live calls passed")
	count, err := store.WinningTicketCount(payer.Address(), 0)
	require.NoError(t, err)
	require.Equal(t, 11, count)
}

func TestPinnedPythonSignerDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Python SDK integration test")
	}
	python, export, root := pinnedPythonSDK(t)
	registry := NewRegistry("", "http://127.0.0.1:0", time.Second, 30*time.Second)
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "discovery-runner", RunnerURL: "https://runner.example", App: "discovery-app", Mode: "single-shot", Status: "ready", Capacity: 1, GPU: &runnerGPU{ID: "0", Name: "H100", VRAMMB: 80000}}))
	policy, err := destination.New("runner", nil)
	require.NoError(t, err)
	server := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	orch := httptest.NewUnstartedServer(server)
	defer orch.Close()
	registry.service = "http://" + orch.Listener.Addr().String()
	orch.Start()
	signerService, err := signer.NewService(paymentTestKey(t))
	require.NoError(t, err)
	defer signerService.Close()
	signerService.SetPaymentChain(paymentTestChain{})
	endpoint := mustURL(t, orch.URL)
	require.NoError(t, signerService.SetDiscovery([]*url.URL{endpoint}, endpoint.Host))
	signerHTTP := httptest.NewServer(signerService)
	defer signerHTTP.Close()
	fixture := filepath.Join(root, "cmd", "livepeer", "testdata", "python_signer_discovery.py")
	command := exec.Command(python, fixture, signerHTTP.URL)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(export, "src"))
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Pinned Python signer discovery passed")
}

func pinnedPythonSDK(t *testing.T) (python, export, root string) {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root = filepath.Clean(filepath.Join(filepath.Dir(testFile), ".."))
	python, export = test.PythonSDK(t, root)
	if python == "" {
		t.Skip("pinned Python runner interpreter unavailable")
	}
	return
}

type advancingPaymentChain struct{ block atomic.Int64 }

func (c *advancingPaymentChain) Snapshot(context.Context) (pm.ChainSnapshot, error) {
	return pm.ChainSnapshot{Block: big.NewInt(c.block.Load()), Round: big.NewInt(5), RoundHash: ethcommon.HexToHash("0x1234")}, nil
}
func (c *advancingPaymentChain) PayerFunds(ctx context.Context, _, _ ethcommon.Address) (pm.PayerFunds, error) {
	snapshot, err := c.Snapshot(ctx)
	return pm.PayerFunds{Snapshot: snapshot, Deposit: big.NewInt(1000000000), Reserve: big.NewInt(1000000000), WithdrawRound: new(big.Int)}, err
}
func (c *advancingPaymentChain) IsActiveAt(context.Context, ethcommon.Address, pm.ChainSnapshot) (bool, error) {
	return true, nil
}

func TestPinnedPythonSustainedPaymentRefresh(t *testing.T) {
	if testing.Short() {
		t.Skip("Python SDK integration")
	}
	python, export, root := pinnedPythonSDK(t)
	chain := new(advancingPaymentChain)
	chain.block.Store(50)
	payer, recipient := paymentTestKey(t), paymentTestKey(t)
	store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
	require.NoError(t, err)
	defer store.Close()
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	engine, err := NewPaymentEngine(store, chain, recipient.Address(), big.NewInt(20), new(big.Int).Add(new(big.Int).Quo(max, big.NewInt(2)), big.NewInt(1)))
	require.NoError(t, err)
	registry := NewRegistry("", "http://127.0.0.1:0", time.Second, time.Minute)
	registry.SetWeiPerUSD(big.NewRat(360000, 1))
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "live", RunnerURL: "https://runner.example", App: "refresh-test", Mode: "persistent", PriceInfo: priceInfo{Price: "1", Currency: "usd", Unit: "hour"}}))
	policy, err := destination.New("runner", nil)
	require.NoError(t, err)
	app := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer app.Close()
	app.SetPayment(engine)
	var refreshes atomic.Int32
	orch := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test/expire-auth" {
			// The SDK treats payment parameters as opaque bytes.
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			info, err := wire.DecodeOrchestratorInfo(data)
			require.NoError(t, err)
			info.Auth.Expiration = time.Now().Add(-time.Minute).Unix()
			_, _ = w.Write(wire.EncodeOrchestratorInfo(info))
			return
		}
		if r.URL.Path == "/test/advance" {
			chain.block.Add(40)
			w.WriteHeader(200)
			return
		}
		if r.URL.Path == "/refresh-payment" {
			refreshes.Add(1)
		}
		app.ServeHTTP(w, r)
	}))
	registry.service = "http://" + orch.Listener.Addr().String()
	orch.Start()
	defer orch.Close()
	service, err := signer.NewService(payer)
	require.NoError(t, err)
	defer service.Close()
	service.SetPaymentChain(chain)
	signed := httptest.NewServer(service)
	defer signed.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, filepath.Join(root, "cmd", "livepeer", "testdata", "python_payment_refresh.py"), orch.URL, signed.URL)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(export, "src"), "PYTHONDONTWRITEBYTECODE=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "signed state preserved")
	require.Equal(t, int32(3), refreshes.Load())
	_, sessions := registry.Counts()
	require.Zero(t, sessions)
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}
