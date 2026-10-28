package orchestrator

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

// recoveryRPC models independent clients against one account's pending pool.
// All mutable RPC state is synchronized so the same scenarios run under -race.
type recoveryRPC struct {
	staleNonceValue                         uint64
	prepareError                            string
	snapshotError, receiptMissingStatus     bool
	block, round                            int64
	inactive, activeError                   bool
	activeRecipient                         common.Address
	onSend                                  func(*types.Transaction)
	mu                                      sync.Mutex
	tickets                                 []*pm.SignedTicket
	sent                                    []*types.Transaction
	pool                                    map[uint64]*types.Transaction
	receipts                                map[common.Hash]*types.Receipt
	used                                    map[common.Hash]bool
	finalized                               bool
	receiptError, receiptMissing            bool
	staleNonce                              bool
	finalizedNonce                          uint64
	finalizedNonceReads                     int
	finalizedNonceError, finalizedNonceNull bool
	rejectNext, loseNextResponse            bool
	mineAfter                               int
	nonceReads                              int
	contractCalls                           int
	rejectSend                              int
	nonceBarrier                            chan struct{}
	receiptHeader                           *types.Header
	server                                  *httptest.Server
}

func newRecoveryRPC(t *testing.T) *recoveryRPC {
	t.Helper()
	f := &recoveryRPC{pool: map[uint64]*types.Transaction{}, receipts: map[common.Hash]*types.Receipt{}, used: map[common.Hash]bool{}, finalized: true, block: 100, round: 1, receiptHeader: &types.Header{Number: big.NewInt(10), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}
func (f *recoveryRPC) serve(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ID     json.RawMessage
		Method string
		Params []json.RawMessage
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	f.mu.Lock()
	var result any = "0x1"
	var failure string
	var barrier chan struct{}
	switch request.Method {
	case "eth_call":
		f.contractCalls++
		var call struct{ Input string }
		_ = json.Unmarshal(request.Params[0], &call)
		selector := func(signature string) bool {
			return strings.HasPrefix(call.Input, fmt.Sprintf("0x%x", crypto.Keccak256([]byte(signature))[:4]))
		}
		result = fmt.Sprintf("0x%064x", 1)
		if selector("lastInitializedRound()") {
			result = fmt.Sprintf("0x%064x", f.round)
		}
		if selector("isActiveTranscoder(address)") {
			f.activeRecipient = common.HexToAddress(call.Input[len(call.Input)-40:])
			if f.activeError {
				failure = "active lookup unavailable"
			}
			if f.inactive {
				result = fmt.Sprintf("0x%064x", 0)
			}
		}
		if selector("usedTickets(bytes32)") {
			hash := common.HexToHash("0x" + call.Input[10:])
			used := f.used[hash]
			// Finalized reads use an EIP-1898 canonical hash, not the moving latest tag.
			if string(request.Params[1]) != "\"latest\"" && !f.finalized {
				used = false
			}
			result = fmt.Sprintf("0x%064x", 0)
			if used {
				result = fmt.Sprintf("0x%064x", 1)
			}
		}
	case "eth_getBlockByNumber":
		var block string
		_ = json.Unmarshal(request.Params[0], &block)
		if block == "latest" && f.snapshotError {
			failure = "snapshot unavailable"
		}
		header := &types.Header{Number: big.NewInt(f.block), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}
		if block == "0xa" {
			header = f.receiptHeader
		}
		if block == "finalized" && !f.finalized {
			header = &types.Header{Number: big.NewInt(9), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}
		}
		result = header
	case "eth_estimateGas":
		failure = f.prepareError
		result = "0x5208"
	case "eth_maxPriorityFeePerGas":
		result = "0x3"
	case "eth_getTransactionCount":
		nonce := f.finalizedNonce
		if string(request.Params[1]) != `"pending"` {
			f.finalizedNonceReads++
			result = fmt.Sprintf("0x%x", nonce)
			if f.finalizedNonceError {
				failure = "finalized nonce lookup unavailable"
			}
			if f.finalizedNonceNull {
				result = nil
			}
			break
		}
		for f.pool[nonce] != nil {
			nonce++
		}
		if f.staleNonce {
			nonce = f.staleNonceValue
		}
		result = fmt.Sprintf("0x%x", nonce)
		f.nonceReads++
		if f.nonceBarrier != nil && f.nonceReads <= 2 {
			barrier = f.nonceBarrier
			if f.nonceReads == 2 {
				close(barrier)
			}
		}
	case "eth_sendRawTransaction":
		var encoded string
		_ = json.Unmarshal(request.Params[0], &encoded)
		raw, err := hex.DecodeString(strings.TrimPrefix(encoded, "0x"))
		if err != nil {
			failure = "bad raw transaction"
			break
		}
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(raw); err != nil {
			failure = "bad transaction"
			break
		}
		f.sent = append(f.sent, tx)
		if f.onSend != nil {
			f.onSend(tx)
		}
		if tx.Nonce() < f.finalizedNonce {
			failure = "nonce too low"
			break
		}
		if f.rejectNext || len(f.sent) == f.rejectSend {
			f.rejectNext = false
			failure = "insufficient funds"
			break
		}
		old := f.pool[tx.Nonce()]
		if old != nil && old.Hash() != tx.Hash() && (tx.GasFeeCap().Cmp(old.GasFeeCap()) <= 0 || tx.GasTipCap().Cmp(old.GasTipCap()) <= 0) {
			failure = "replacement transaction underpriced"
			break
		}
		f.pool[tx.Nonce()] = tx
		result = tx.Hash().Hex()
		if f.mineAfter > 0 && len(f.sent) >= f.mineAfter {
			f.mineLocked(tx, true)
		}
		if f.loseNextResponse {
			f.loseNextResponse = false
			f.mu.Unlock()
			http.Error(w, "lost response", http.StatusServiceUnavailable)
			return
		}
	case "eth_getTransactionReceipt":
		var hash common.Hash
		_ = json.Unmarshal(request.Params[0], &hash)
		result = f.receipts[hash]
		if f.receipts[hash] != nil && f.receiptMissingStatus {
			encoded, _ := json.Marshal(result)
			var fields map[string]any
			_ = json.Unmarshal(encoded, &fields)
			delete(fields, "status")
			result = fields
		}
		if f.receiptMissing {
			result = nil
		}
		if f.receiptError {
			failure = "receipt lookup unavailable"
		}
	case "eth_getTransactionByHash":
		var hash common.Hash
		_ = json.Unmarshal(request.Params[0], &hash)
		result = nil
		for _, tx := range f.sent {
			pending := f.pool[tx.Nonce()]
			if tx.Hash() == hash && (f.receipts[hash] != nil || (pending != nil && pending.Hash() == hash)) {
				encoded, _ := json.Marshal(tx)
				var fields map[string]any
				_ = json.Unmarshal(encoded, &fields)
				if receipt := f.receipts[hash]; receipt != nil {
					fields["blockNumber"] = "0xa"
					fields["blockHash"] = f.receiptHeader.Hash()
				}
				result = fields
				break
			}
		}
	default:
		failure = "unexpected RPC " + request.Method
	}
	f.mu.Unlock()
	if barrier != nil {
		select {
		case <-barrier:
		case <-r.Context().Done():
			return
		}
	}
	response := map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}
	if failure != "" {
		delete(response, "result")
		response["error"] = map[string]any{"code": -32000, "message": failure}
	}
	_ = json.NewEncoder(w).Encode(response)
}
func (f *recoveryRPC) mineLocked(tx *types.Transaction, success bool) {
	status := uint64(0)
	if success {
		status = 1
	}
	f.receipts[tx.Hash()] = &types.Receipt{TxHash: tx.Hash(), Status: status, BlockNumber: big.NewInt(10), BlockHash: f.receiptHeader.Hash(), Logs: []*types.Log{}}
	if success {
		for _, ticket := range f.tickets {
			if strings.Contains(hex.EncodeToString(tx.Data()), hex.EncodeToString(ticket.RecipientRandHash[:])) {
				f.used[ticket.Hash()] = true
			}
		}
	}
}
func (f *recoveryRPC) transactions() []*types.Transaction {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*types.Transaction(nil), f.sent...)
}
func (f *recoveryRPC) mine(tx *types.Transaction, success bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mineLocked(tx, success)
}
func (f *recoveryRPC) chain(t *testing.T) eth.PaymentChain {
	t.Helper()
	rpc, err := eth.OpenRPC(f.server.URL)
	require.NoError(t, err)
	t.Cleanup(rpc.Close)
	c, err := eth.NewContracts(rpc, common.HexToAddress("0x1000"))
	require.NoError(t, err)
	return eth.PaymentChain{Contracts: c}
}
func recoveryKey(t *testing.T) *eth.Key {
	t.Helper()
	path, password := test.WriteFixedKeystore(t)
	key, err := eth.OpenKeystoreFile(path, password)
	require.NoError(t, err)
	return key
}
func (f *recoveryRPC) store(t *testing.T, id byte) (*RedeemerDB, *pm.SignedTicket) {
	t.Helper()
	store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: common.HexToAddress("0x1234"), Recipient: common.HexToAddress("0x5678"), FaceValue: big.NewInt(1000), WinProb: big.NewInt(100), TicketNonce: 1, RecipientRandHash: crypto.Keccak256Hash([]byte{id}), CreationRound: 1, ParamsExpirationBlock: big.NewInt(10)}, Sig: []byte{id, 2, 3}, RecipientRand: big.NewInt(int64(id))}
	require.NoError(t, store.StoreWinningTicket(ticket))
	f.mu.Lock()
	f.tickets = append(f.tickets, ticket)
	f.mu.Unlock()
	return store, ticket
}
func ticketPhase(t *testing.T, s *RedeemerDB, sig []byte) string {
	t.Helper()
	var phase string
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_attempts WHERE sig=?", sig).Scan(&phase))
	return phase
}
func tickRecovery(t *testing.T, s *RedeemerDB, c eth.PaymentChain, k *eth.Key) {
	t.Helper()
	require.Empty(t, ProcessRedemptions(t.Context(), s, c, k, big.NewInt(1)))
}

// newRecoveryTest shares setup without starting recovery or changing RPC behavior.
func newRecoveryTest(t *testing.T, id byte) (*recoveryRPC, *RedeemerDB, *pm.SignedTicket, eth.PaymentChain, *eth.Key) {
	t.Helper()
	f := newRecoveryRPC(t)
	store, ticket := f.store(t, id)
	return f, store, ticket, f.chain(t), recoveryKey(t)
}

func TestSharedWalletRedemptionRecovery(t *testing.T) {
	f := newRecoveryRPC(t)
	f.nonceBarrier = make(chan struct{})
	key := recoveryKey(t)
	a, ta := f.store(t, 1)
	b, tb := f.store(t, 2)
	ca, cb := f.chain(t), f.chain(t)
	results := make(chan []error, 2)
	go func() { results <- ProcessRedemptions(t.Context(), a, ca, key, big.NewInt(1)) }()
	go func() { results <- ProcessRedemptions(t.Context(), b, cb, key, big.NewInt(1)) }()
	first, second := <-results, <-results
	require.Equal(t, 1, len(first)+len(second))
	sent := f.transactions()
	require.Len(t, sent, 2)
	require.EqualValues(t, 0, sent[0].Nonce())
	require.EqualValues(t, 0, sent[1].Nonce())
	require.NotEqual(t, sent[0].Hash(), sent[1].Hash())
	// The losing process discovers pending nonce 1 and retries its own ticket.
	loser, lc, lt := a, ca, ta
	if ticketPhase(t, b, tb.Sig) == "retryable" {
		loser, lc, lt = b, cb, tb
	}
	floor, err := loser.SavedNonceFloor(t.Context(), key.Address())
	require.NoError(t, err)
	require.Zero(t, floor)
	tickRecovery(t, loser, lc, key)
	sent = f.transactions()
	require.Len(t, sent, 3)
	require.EqualValues(t, 1, sent[2].Nonce())
	require.NotEqual(t, "retryable", ticketPhase(t, loser, lt.Sig))
	f.mine(sent[0], true)
	f.mine(sent[2], true)
	tickRecovery(t, a, ca, key)
	tickRecovery(t, b, cb, key)
	require.Equal(t, "confirmed", ticketPhase(t, a, ta.Sig))
	require.Equal(t, "confirmed", ticketPhase(t, b, tb.Sig))
	history, err := loser.Redemptions()
	require.NoError(t, err)
	require.Len(t, history, 2)
}

func TestRejectedRedemptionDoesNotSkipNonce(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 3)
	f.rejectNext = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	require.Equal(t, "retryable", ticketPhase(t, s, ticket.Sig))
	tickRecovery(t, s, c, key)
	sent := f.transactions()
	require.Len(t, sent, 2)
	require.EqualValues(t, 0, sent[1].Nonce())
	require.Equal(t, sent[0].Hash(), sent[1].Hash())
}

func TestRedemptionReplacementAndFinality(t *testing.T) {
	for _, winner := range []int{0, 1} {
		t.Run(fmt.Sprintf("winner=%d", winner), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 4)
			f.finalized = false
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: 1}))
			tickRecovery(t, s, c, key)
			require.Eventually(t, func() bool { return len(f.transactions()) == 2 }, time.Second, 5*time.Millisecond)
			sent := f.transactions()
			require.Equal(t, sent[0].Nonce(), sent[1].Nonce())
			require.Equal(t, sent[0].Data(), sent[1].Data())
			require.Equal(t, big.NewInt(224), sent[1].GasFeeCap())
			require.Equal(t, big.NewInt(4), sent[1].GasTipCap())
			f.mine(sent[winner], true)
			tickRecovery(t, s, c, key)
			require.NotEqual(t, "confirmed", ticketPhase(t, s, ticket.Sig))
			require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "mined" }, time.Second, 5*time.Millisecond)
			f.mu.Lock()
			f.finalized = true
			f.mu.Unlock()
			tickRecovery(t, s, c, key)
			require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
			var hash string
			require.NoError(t, s.db.QueryRow("SELECT tx_hash FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&hash))
			require.Equal(t, sent[winner].Hash().Hex(), hash)
			items, err := s.Redemptions()
			require.NoError(t, err)
			require.Len(t, items, 2)
			for i, tx := range sent {
				require.Equal(t, tx.Hash().Hex(), items[i].Hash)
				require.Error(t, RetryRedemption(t.Context(), s, c.Contracts, tx.Hash()))
			}
			require.Len(t, f.transactions(), 2)
		})
	}
}

func TestRedemptionReplacementFailuresRemainQueued(t *testing.T) {
	for _, scenario := range []string{"limit", "ceiling", "rejection", "persistence"} {
		t.Run(scenario, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 6)
			s.metrics = observability.New("orchestrator")
			options := RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: 1}
			if scenario == "limit" {
				options.MaxReplacements = 0
			}
			if scenario == "rejection" {
				f.rejectSend = 2
				options.MaxReplacements = 3
			}
			if scenario == "ceiling" {
				c.Contracts.MaxFeePerGas = big.NewInt(210)
			}
			if scenario == "persistence" {
				_, err := s.db.Exec(`CREATE TRIGGER reject_replacement BEFORE INSERT ON redemption_transactions WHEN NEW.replaces_hash IS NOT NULL BEGIN SELECT RAISE(ABORT,'storage failure'); END`)
				require.NoError(t, err)
			}
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, options))
			nextResult := captureRecoveryResults(t, s)
			tickRecovery(t, s, c, key)
			require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "retryable" }, time.Second, 5*time.Millisecond)
			expected := 1
			if scenario == "rejection" {
				expected = 2
			}
			require.Len(t, f.transactions(), expected)
			if scenario == "rejection" {
				var acknowledged bool
				require.NoError(t, s.db.QueryRow("SELECT acknowledged FROM redemption_transactions WHERE replaces_hash IS NOT NULL").Scan(&acknowledged))
				require.False(t, acknowledged, "the rejected replacement must stop the remaining replacement budget")
			}
			// A timeout/rejected replacement does not settle the still-live ticket.
			var redeemed *string
			require.NoError(t, s.db.QueryRow("SELECT redeemed_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&redeemed))
			require.Nil(t, redeemed)
			result := nextResult()
			require.Error(t, result.Error())
			// A rejected replacement commits retryable before the callback moves
			// the still-submitted predecessor into retryable as well.
			require.EqualValues(t, expected, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_errors_total", nil))
			require.NoError(t, s.recovery.recordResult(t.Context(), result))
			require.EqualValues(t, expected, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}), "replaying a completed callback must not emit another transition")
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_errors_total", nil), "replaying a completed callback must not emit another failure")
			require.Zero(t, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
		})
	}
}

func TestLostResponseUsesFinalizedTicketGuard(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 7)
	f.loseNextResponse = true
	f.finalized = false
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	sent := f.transactions()
	f.mine(sent[0], true)
	tickRecovery(t, s, c, key)
	require.Equal(t, "awaiting_finality", ticketPhase(t, s, ticket.Sig))
	require.Len(t, f.transactions(), 1)
	f.mu.Lock()
	f.finalized = true
	f.mu.Unlock()
	tickRecovery(t, s, c, key)
	require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
	require.Len(t, f.transactions(), 1)
	floor, err := s.SavedNonceFloor(t.Context(), key.Address())
	require.NoError(t, err)
	require.EqualValues(t, 1, floor, "a finalized receipt proves acceptance even when the send response was lost")
}

func TestUsedTicketGuardRecoversAfterReorg(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 8)
	s.metrics = observability.New("orchestrator")
	f.finalized = false
	f.used[ticket.Hash()] = true
	tickRecovery(t, s, c, key)
	require.Equal(t, "awaiting_finality", ticketPhase(t, s, ticket.Sig))
	require.Empty(t, f.transactions())
	tickRecovery(t, s, c, key)
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "awaiting_finality"}))
	f.mu.Lock()
	delete(f.used, ticket.Hash())
	f.mu.Unlock()
	tickRecovery(t, s, c, key)
	require.Len(t, f.transactions(), 1)
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	require.Zero(t, test.MetricSum(t, s.metrics.Registry, "redemption_errors_total", nil), "a reorg is not a service failure")
}

func reopenRecoveryStore(t *testing.T, s *RedeemerDB) *RedeemerDB {
	t.Helper()
	var seq int
	var name, path string
	require.NoError(t, s.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path))
	require.NoError(t, s.Close())
	next, err := OpenRedeemerDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, next.Close()) })
	return next
}

func TestRedemptionRestartRestoresTransactionCursor(t *testing.T) {
	for _, boundary := range []string{"prepared", "sending", "submitted", "replacement-prepared", "replacement-submitted"} {
		t.Run(boundary, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 10)
			if boundary == "prepared" {
				prepared, err := c.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
				require.NoError(t, err)
				require.NoError(t, s.recordPrepared(t.Context(), ticket, prepared))
			} else {
				options := RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}
				if boundary == "replacement-submitted" {
					options.Timeout = 100 * time.Millisecond
				}
				require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, options))
				tickRecovery(t, s, c, key)
				if boundary == "replacement-submitted" {
					require.Eventually(t, func() bool {
						var count int
						err := s.db.QueryRow("SELECT count(*) FROM redemption_transactions WHERE acknowledged=1").Scan(&count)
						return err == nil && count == 2
					}, time.Second, time.Millisecond)
				}
			}
			// Stop the old runtime before editing a durable crash-boundary fixture.
			s = reopenRecoveryStore(t, s)
			if boundary == "sending" {
				_, err := s.db.Exec("UPDATE redemption_attempts SET phase='broadcast'")
				require.NoError(t, err)
				_, err = s.db.Exec("UPDATE redemption_transactions SET phase='sending',acknowledged=0")
				require.NoError(t, err)
			}
			if boundary == "replacement-prepared" {
				first := f.transactions()[0]
				replacement := recoveryReplacement(t, key, first)
				identity, err := signedIdentity(replacement)
				require.NoError(t, err)
				require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, first.Hash().Hex()))
			}
			expectedSends := 1
			if boundary == "sending" || strings.HasPrefix(boundary, "replacement-") {
				expectedSends = 2
			}
			var originalDeadline string
			if boundary == "sending" {
				require.NoError(t, s.db.QueryRow("SELECT broadcast_at FROM redemption_transactions").Scan(&originalDeadline))
			}
			options := RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}
			if boundary == "replacement-submitted" {
				options.Timeout = time.Nanosecond
			}
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, options))
			tickRecovery(t, s, c, key)
			require.Len(t, f.transactions(), expectedSends)
			if boundary == "replacement-submitted" {
				require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "retryable" }, time.Second, time.Millisecond)
				require.Len(t, f.transactions(), 2, "restart must retain the spent replacement budget")
			}
			if boundary == "sending" {
				var at string
				require.NoError(t, s.db.QueryRow("SELECT broadcast_at FROM redemption_transactions").Scan(&at))
				require.Equal(t, originalDeadline, at, "restarting an uncertain send must preserve its timeout")
				sent := f.transactions()
				require.Equal(t, sent[0].Hash(), sent[1].Hash())
			}
		})
	}
}

func TestRedemptionRecoveryBeforeAndAfterBroadcast(t *testing.T) {
	for _, failure := range []string{"preparation", "prepared-crash", "uncertain-broadcast"} {
		t.Run(failure, func(t *testing.T) {
			f, store, ticket, chain, key := newRecoveryTest(t, 1)
			f.finalizedNonce = 1
			if failure == "preparation" {
				f.prepareError = "preparation unavailable"
			}
			f.loseNextResponse = failure == "uncertain-broadcast"
			snapshot, err := chain.Snapshot(t.Context())
			require.NoError(t, err)
			var prepared eth.SignedTransaction
			if failure == "prepared-crash" {
				prepared, err = chain.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
				require.NoError(t, err)
				require.NoError(t, store.recordPrepared(t.Context(), ticket, prepared))
			} else {
				require.NotEmpty(t, RedeemPending(t.Context(), store, chain, key, big.NewInt(1), snapshot))
			}
			store = reopenRecoveryStore(t, store)
			f.mu.Lock()
			f.prepareError = ""
			f.staleNonce = true
			f.mu.Unlock()
			restartKey := key
			if failure == "prepared-crash" {
				restartKey = paymentTestKey(t)
			}
			require.Empty(t, RedeemPending(t.Context(), store, chain, restartKey, big.NewInt(1), snapshot))
			sent := f.transactions()
			expectedSends := 1
			if failure == "uncertain-broadcast" {
				expectedSends = 2
			}
			require.Len(t, sent, expectedSends, "retryable sends recover automatically")
			if failure == "prepared-crash" {
				raw, err := sent[0].MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, prepared.Raw, raw, "rotated-key recovery must reuse the exact signed bytes")
			} else if failure == "uncertain-broadcast" {
				require.Equal(t, sent[0].Hash(), sent[1].Hash(), "stale pending reads must retain the saved transaction")
			}
			f.mu.Lock()
			f.mineLocked(sent[len(sent)-1], true)
			f.receiptMissingStatus = true
			f.mu.Unlock()
			before, err := store.Redemptions()
			require.NoError(t, err)
			require.Len(t, before, 1)
			require.NotEmpty(t, ReconcileSubmitted(t.Context(), store, chain), "missing receipt status must fail reconciliation")
			after, err := store.Redemptions()
			require.NoError(t, err)
			require.Equal(t, before, after, "malformed receipts must leave durable attempts unresolved")
			next := nextRecoveryTicket(ticket, 4)
			next.RecipientRandHash = ticket.RecipientRandHash
			next.TicketNonce = 2
			require.NoError(t, store.StoreWinningTicket(next))
			f.mu.Lock()
			f.receiptMissingStatus = false
			f.snapshotError = true
			f.mu.Unlock()
			require.NotEmpty(t, ProcessRedemptions(t.Context(), store, chain, restartKey, big.NewInt(1)))
			require.Equal(t, "confirmed", ticketPhase(t, store, ticket.Sig), "reconcile despite an unavailable latest snapshot")
			require.Len(t, f.transactions(), expectedSends)
			queued, err := store.PendingPayers()
			require.NoError(t, err)
			require.Equal(t, []common.Address{ticket.PayerAddress}, queued)
			require.Error(t, RetryRedemption(t.Context(), store, chain.Contracts, common.HexToHash(before[0].Hash)))
			if failure == "prepared-crash" {
				floor, err := store.SavedNonceFloor(t.Context(), key.Address())
				require.NoError(t, err)
				require.EqualValues(t, 2, floor)
				floor, err = store.SavedNonceFloor(t.Context(), restartKey.Address())
				require.NoError(t, err)
				require.Zero(t, floor)
				store = reopenRecoveryStore(t, store)
				f.mu.Lock()
				f.snapshotError = false
				f.mu.Unlock()
				chain = f.chain(t)
				require.Empty(t, RedeemPending(t.Context(), store, chain, key, big.NewInt(1), snapshot))
				sent = f.transactions()
				require.EqualValues(t, 2, sent[len(sent)-1].Nonce(), "confirmed history must advance a fresh client's nonce floor")
			}
		})
	}
}

func TestTerminalAttemptRetainsUnresolvedSibling(t *testing.T) {
	for _, tc := range []struct {
		phase    string
		minRound int64
	}{{"reverted", 0}, {"invalid", 0}, {"expired", 0}, {"expired", 2}} {
		t.Run(fmt.Sprintf("%s/min-round=%d", tc.phase, tc.minRound), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 11)
			f.loseNextResponse = true
			require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			tickRecovery(t, s, c, key)
			sent := f.transactions()
			require.Len(t, sent, 2)
			f.mine(sent[1], false)
			require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			require.Equal(t, "reverted", ticketPhase(t, s, ticket.Sig))
			if tc.phase != "reverted" {
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, tc.phase))
			}
			// The provider has not accounted for the earlier signed identity. The ticket
			// stops retrying, but cannot release its collateral based on one failed hash.
			next := nextRecoveryTicket(ticket, 12)
			next.CreationRound = max(ticket.CreationRound, tc.minRound)
			require.ErrorIs(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, tc.minRound, big.NewInt(1000), []*pm.SignedTicket{next}), pm.ErrPayerUnavailable)
			f.mine(sent[0], true)
			tickRecovery(t, s, c, key)
			require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
			require.NoError(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, tc.minRound, big.NewInt(1000), []*pm.SignedTicket{next}))
		})
	}
}

func TestOrphanedInclusionRecoversSameNonce(t *testing.T) {
	for _, replacements := range []int{0, 1} {
		t.Run(fmt.Sprintf("replacements=%d", replacements), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 13)
			f.finalized = false
			f.mineAfter = 1
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: replacements}))
			tickRecovery(t, s, c, key)
			require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "mined" }, time.Second, time.Millisecond)
			f.mu.Lock()
			f.mineAfter = 0
			f.receipts = map[common.Hash]*types.Receipt{}
			f.used = map[common.Hash]bool{}
			f.mu.Unlock()
			tickRecovery(t, s, c, key)
			if replacements == 0 {
				require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "retryable" }, time.Second, time.Millisecond)
				status, err := s.Redemptions()
				require.NoError(t, err)
				require.Equal(t, "retryable", status[0].Phase)
				require.NotEmpty(t, status[0].Error)
				require.NoError(t, RetryRedemption(t.Context(), s, c.Contracts, f.transactions()[0].Hash()))
			} else {
				require.Eventually(t, func() bool { return len(f.transactions()) == 2 }, time.Second, time.Millisecond)
			}
			sent := f.transactions()
			require.Len(t, sent, 2)
			require.Equal(t, sent[0].Nonce(), sent[1].Nonce())
		})
	}
}

func TestRedemptionPersistencePreventsInitialSend(t *testing.T) {
	f, s, _, c, key := newRecoveryTest(t, 14)
	_, err := s.db.Exec(`CREATE TRIGGER reject_identity BEFORE INSERT ON redemption_transactions BEGIN SELECT RAISE(ABORT,'storage unavailable'); END`)
	require.NoError(t, err)
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	require.Empty(t, f.transactions())
	_, err = s.db.Exec("DROP TRIGGER reject_identity")
	require.NoError(t, err)
	tickRecovery(t, s, c, key)
	require.EqualValues(t, 0, f.transactions()[0].Nonce())
}

func TestManualRedemptionRetryCannotDisplaceActiveIdentity(t *testing.T) {
	f, s, _, c, key := newRecoveryTest(t, 15)
	f.loseNextResponse = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	first := f.transactions()[0]
	require.NoError(t, RetryRedemption(t.Context(), s, c.Contracts, first.Hash()))
	require.Equal(t, first.Hash(), f.transactions()[1].Hash())
	// Send another identity only after upstream timeout returns this ticket.
	_, err := s.db.Exec("UPDATE redemption_attempts SET phase='retryable'")
	require.NoError(t, err)
	tickRecovery(t, s, c, key)
	require.Len(t, f.transactions(), 3)
	require.Error(t, RetryRedemption(t.Context(), s, c.Contracts, first.Hash()))
	require.Len(t, f.transactions(), 3)
}

func TestExpiredAttemptDoesNotReopenRandomness(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 16)
	f.rejectNext = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, "expired"))
	next := nextRecoveryTicket(ticket, 17)
	next.RecipientRandHash = ticket.RecipientRandHash
	next.TicketNonce = 2
	require.ErrorIs(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, ticket.RecipientRandHash, 0, big.NewInt(10000), []*pm.SignedTicket{next}), ErrInvalidPayment)
}

func recoveryReplacement(t *testing.T, key *eth.Key, first *types.Transaction) *types.Transaction {
	t.Helper()
	next, err := key.SignTxForChain(types.NewTx(&types.DynamicFeeTx{ChainID: first.ChainId(), Nonce: first.Nonce(), Gas: first.Gas(), GasFeeCap: big.NewInt(224), GasTipCap: big.NewInt(4), To: first.To(), Data: first.Data(), Value: first.Value()}), first.ChainId())
	require.NoError(t, err)
	return next
}

func TestMinedFailureOutlivesInclusionDeadline(t *testing.T) {
	for _, tc := range []struct {
		spent, limit                 int
		receiptError, receiptMissing bool
	}{{0, 0, false, false}, {1, 1, false, false}, {0, 0, true, false}, {0, 0, false, true}, {1, 1, false, true}, {0, 1, false, false}, {1, 2, false, false}} {
		t.Run(fmt.Sprintf("spent=%d/limit=%d/receipt-error=%t/missing=%t", tc.spent, tc.limit, tc.receiptError, tc.receiptMissing), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 18)
			f.finalized = false
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: tc.limit}))
			tickRecovery(t, s, c, key)
			s = reopenRecoveryStore(t, s)
			last := f.transactions()[0]
			if tc.spent > 0 {
				replacement := recoveryReplacement(t, key, last)
				identity, err := signedIdentity(replacement)
				require.NoError(t, err)
				require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, last.Hash().Hex()))
				require.NoError(t, RetryRedemption(t.Context(), s, c.Contracts, replacement.Hash()))
				last = replacement
			}
			included := last
			if tc.receiptMissing && tc.spent > 0 {
				included = f.transactions()[0]
			}
			f.mine(included, false)
			_, err := s.db.Exec("UPDATE redemption_transactions SET broadcast_at=?", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano))
			require.NoError(t, err)
			_, err = s.db.Exec("UPDATE redemption_attempts SET phase='mined'")
			require.NoError(t, err)
			_, err = s.db.Exec("UPDATE redemption_transactions SET phase='mined' WHERE hash=?", included.Hash().Hex())
			require.NoError(t, err)
			f.mu.Lock()
			f.receiptError = tc.receiptError
			f.receiptMissing = tc.receiptMissing
			f.mu.Unlock()
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Nanosecond, MaxReplacements: tc.limit}))
			failures := ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1))
			require.Equal(t, tc.receiptError, len(failures) > 0)
			require.Eventually(t, func() bool { s.recovery.mu.Lock(); defer s.recovery.mu.Unlock(); return len(s.recovery.tracking) == 0 }, time.Second, time.Millisecond)
			require.Equal(t, "mined", ticketPhase(t, s, ticket.Sig), "canonical inclusion must survive an exhausted inclusion timeout")
			if included.Hash() != last.Hash() {
				var phase string
				require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", last.Hash().Hex()).Scan(&phase))
				require.Equal(t, "submitted", phase, "an included predecessor must not mark the pending replacement mined")
			}
			ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1))
			require.Len(t, f.transactions(), tc.spent+1, "unfinalized failure must not start another redemption")
		})
	}
}

func TestTerminalTicketKeepsUnresolvedReceipts(t *testing.T) {
	for _, phase := range []string{"expired", "invalid"} {
		for _, included := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/attempt=%d", phase, included), func(t *testing.T) {
				f, s, ticket, c, key := newRecoveryTest(t, 19)
				f.loseNextResponse = true
				f.finalized = false
				require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
				tickRecovery(t, s, c, key)
				s = reopenRecoveryStore(t, s)
				f.mine(f.transactions()[included], true)
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, phase))
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
				require.Equal(t, phase, ticketPhase(t, s, ticket.Sig), "unfinalized inclusion must not settle a terminal ticket")
				f.mu.Lock()
				f.finalized = true
				f.mu.Unlock()
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
				require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
			})
		}
	}
}

func TestRevertedReplacementReleasesSameNonceLiability(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 20)
	tickRecovery(t, s, c, key)
	s = reopenRecoveryStore(t, s)
	first := f.transactions()[0]
	replacement := recoveryReplacement(t, key, first)
	identity, err := signedIdentity(replacement)
	require.NoError(t, err)
	require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, first.Hash().Hex()))
	require.NoError(t, RetryRedemption(t.Context(), s, c.Contracts, replacement.Hash()))
	f.mine(replacement, false)
	require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, c))
	require.Equal(t, "reverted", ticketPhase(t, s, ticket.Sig))
	next := nextRecoveryTicket(ticket, 21)
	require.NoError(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 0, big.NewInt(1000), []*pm.SignedTicket{next}), "a finalized replacement consumes every sibling with the same sender and nonce")
}

func TestManualRetryFailureKeepsActiveMonitor(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 101)
	s.metrics = observability.New("orchestrator")
	require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}))
	tickRecovery(t, s, c, key)
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "broadcast"}))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "submitted"}))
	first := f.transactions()[0]
	f.mu.Lock()
	f.rejectNext = true
	f.mu.Unlock()
	require.Error(t, RetryRedemption(t.Context(), s, c.Contracts, first.Hash()))
	require.Equal(t, "submitted", ticketPhase(t, s, ticket.Sig))
	tickRecovery(t, s, c, key)
	sent := f.transactions()
	s.recovery.mu.Lock()
	roots := len(s.recovery.tracking)
	s.recovery.mu.Unlock()
	require.Equal(t, 1, roots)
	require.Len(t, sent, 2, "an identical-byte manual retry must not admit a fresh nonce while the original monitor is active")
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "broadcast"}))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "submitted"}))
}

func TestConsumedNonceResolvesOtherTicketHistory(t *testing.T) {
	f := newRecoveryRPC(t)
	s, a := f.store(t, 103)
	s.metrics = observability.New("orchestrator")
	ca, cb, key := f.chain(t), f.chain(t), recoveryKey(t)
	b := nextRecoveryTicket(a, 104)
	b.PayerAddress = common.HexToAddress("0x9999")
	require.NoError(t, s.StoreWinningTicket(b))
	a0, err := ca.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(a))
	require.NoError(t, err)
	require.NoError(t, s.recordPrepared(t.Context(), a, a0))
	f.mu.Lock()
	f.rejectNext = true
	f.mu.Unlock()
	require.Error(t, broadcastRedemption(t.Context(), s, ca.Contracts, a.Sig, a0))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	b0, err := cb.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(b))
	require.NoError(t, err)
	require.Equal(t, a0.Nonce, b0.Nonce)
	require.NoError(t, s.recordPrepared(t.Context(), b, b0))
	require.NoError(t, broadcastRedemption(t.Context(), s, cb.Contracts, b.Sig, b0))
	a1, err := ca.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(a))
	require.NoError(t, err)
	require.EqualValues(t, 1, a1.Nonce)
	require.NoError(t, s.recordPrepared(t.Context(), a, a1))
	require.NoError(t, broadcastRedemption(t.Context(), s, ca.Contracts, a.Sig, a1))
	// A durable sibling on the same sender/nonce must become retryable when B
	// finalizes, even though the sibling itself has no finalized receipt.
	sibling := nextRecoveryTicket(a, 108)
	require.NoError(t, s.StoreWinningTicket(sibling))
	require.NoError(t, s.recordPrepared(t.Context(), sibling, eth.SignedTransaction{Hash: common.HexToHash("0xc"), Raw: []byte{1}, From: b0.From, Nonce: b0.Nonce}))
	sent := f.transactions()
	f.mine(sent[1], true)
	f.mine(sent[2], false)
	require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, ca))
	require.Equal(t, "confirmed", ticketPhase(t, s, b.Sig))
	require.Equal(t, "reverted", ticketPhase(t, s, a.Sig))
	require.Equal(t, "retryable", ticketPhase(t, s, sibling.Sig))
	require.EqualValues(t, 2, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
	require.NoError(t, s.finishTicket(t.Context(), b.Sig, "confirmed", b0.Hash.Hex()))
	require.EqualValues(t, 2, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
	require.NoError(t, s.setTicketPhase(t.Context(), sibling.Sig, "invalid"))
	next := nextRecoveryTicket(a, 105)
	require.NoError(t, s.storeWinningTickets(t.Context(), a.PayerAddress, next.RecipientRandHash, 0, big.NewInt(1000), []*pm.SignedTicket{next}), "B consumed nonce zero and A's nonce one failed; A has no remaining liability")
}

func TestMultipleFinalizedFailuresResolveEveryGroup(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 106)
	var originals []*types.Transaction
	for i := 0; i < 2; i++ {
		prepared, err := c.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
		require.NoError(t, err)
		require.NoError(t, s.recordPrepared(t.Context(), ticket, prepared))
		require.NoError(t, broadcastRedemption(t.Context(), s, c.Contracts, ticket.Sig, prepared))
		first := f.transactions()[2*i]
		originals = append(originals, first)
		replacement := recoveryReplacement(t, key, first)
		identity, err := signedIdentity(replacement)
		require.NoError(t, err)
		require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, first.Hash().Hex()))
		require.NoError(t, broadcastRedemption(t.Context(), s, c.Contracts, ticket.Sig, identity))
		if i == 0 {
			_, err = s.db.Exec("UPDATE redemption_attempts SET phase='retryable' WHERE sig=?", ticket.Sig)
			require.NoError(t, err)
		}
	}
	for _, tx := range originals {
		f.mine(tx, false)
	}
	require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, c))
	history, err := s.Redemptions()
	require.NoError(t, err)
	require.Len(t, history, 4)
	next := nextRecoveryTicket(ticket, 107)
	require.NoError(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 0, big.NewInt(1000), []*pm.SignedTicket{next}), "both finalized failed nonces must resolve their replacement siblings")
}

func TestLiveAcknowledgementRestoresNonce(t *testing.T) {
	for _, mode := range []string{"manual", "manual-external-handle", "finalized"} {
		t.Run(mode, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 111)
			f.loseNextResponse = true
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}))
			require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			first := f.transactions()[0]
			if mode != "finalized" {
				retryStore := s
				if mode == "manual-external-handle" {
					var seq int
					var name, path string
					require.NoError(t, s.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path))
					var err error
					retryStore, err = OpenRedeemerDB(path)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, retryStore.Close()) })
				}
				require.NoError(t, RetryRedemption(t.Context(), retryStore, c.Contracts, first.Hash()))
			} else {
				f.mine(first, true)
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
			}
			floor, err := s.SavedNonceFloor(t.Context(), key.Address())
			require.NoError(t, err)
			require.EqualValues(t, 1, floor)
			next := nextRecoveryTicket(ticket, 112)
			next.PayerAddress = common.HexToAddress("0x9999")
			require.NoError(t, s.StoreWinningTicket(next))
			f.mu.Lock()
			f.staleNonce = true
			f.mu.Unlock()
			tickRecovery(t, s, c, key)
			sent := f.transactions()
			require.EqualValues(t, 1, sent[len(sent)-1].Nonce(), "new submission must honor a durable acknowledgement received since this worker started")
		})
	}
}

func TestUsedTransitionRecoversAfterStorageFailure(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 222)
	f.loseNextResponse = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	s = reopenRecoveryStore(t, s)
	f.mu.Lock()
	f.used[ticket.Hash()] = true
	f.mu.Unlock()
	_, err := s.db.Exec(`CREATE TRIGGER fail_settlement BEFORE UPDATE OF redeemed_at ON winning_tickets BEGIN SELECT RAISE(ABORT,'injected settlement failure'); END`)
	require.NoError(t, err)
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	require.Equal(t, "retryable", ticketPhase(t, s, ticket.Sig), "failed settlement must roll back the terminal summary")
	_, err = s.db.Exec("DROP TRIGGER fail_settlement")
	require.NoError(t, err)
	s = reopenRecoveryStore(t, s)
	tickRecovery(t, s, c, key)
	var redeemed bool
	require.NoError(t, s.db.QueryRow("SELECT redeemed_at IS NOT NULL FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&redeemed))
	if !redeemed {
		t.Errorf("summary=%s but redeemed_at remains NULL even after restart and retry", ticketPhase(t, s, ticket.Sig))
	}
	next := nextRecoveryTicket(ticket, 223)
	require.NoError(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 0, big.NewInt(1000), []*pm.SignedTicket{next}), "a finalized-used ticket must not strand collateral")
}

func TestRejectedReplacementKeepsPredecessorReconcilable(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 225)
	require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}))
	tickRecovery(t, s, c, key)
	original := f.transactions()[0]
	s = reopenRecoveryStore(t, s)
	replacement := recoveryReplacement(t, key, original)
	identity, err := signedIdentity(replacement)
	require.NoError(t, err)
	require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, original.Hash().Hex()))
	f.mu.Lock()
	f.rejectNext = true
	f.mu.Unlock()
	require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}))
	require.NotEmpty(t, s.recovery.resume(t.Context()))
	require.Equal(t, "retryable", ticketPhase(t, s, ticket.Sig))
	var phase string
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", original.Hash().Hex()).Scan(&phase))
	require.Equal(t, "submitted", phase)
	f.mine(original, true)
	require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
	require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
}

func TestFinalizedUsePollingRetainsOnlyUnresolvedWork(t *testing.T) {
	for _, tc := range []struct {
		name              string
		unresolved, mined bool
	}{{"resolved revert", false, false}, {"unresolved revert", true, false}, {"mined", false, true}} {
		unresolved := tc.unresolved
		t.Run(tc.name, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 226)
			f.loseNextResponse = unresolved
			if tc.mined {
				f.mineAfter = 1
				f.finalized = false
			}
			if unresolved {
				require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			}
			tickRecovery(t, s, c, key)
			if tc.mined {
				require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "mined" }, time.Second, time.Millisecond)
			}
			s = reopenRecoveryStore(t, s)
			if tc.mined {
				f.mu.Lock()
				f.finalized = true
				f.receiptMissing = true
				f.mu.Unlock()
			} else {
				sent := f.transactions()
				f.mine(sent[len(sent)-1], false)
				require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, c))
				require.Equal(t, "reverted", ticketPhase(t, s, ticket.Sig))
			}
			r := &redemptionRecovery{store: s, chain: c}
			f.mu.Lock()
			before := f.contractCalls
			f.mu.Unlock()
			require.Empty(t, r.reconcileUsed(t.Context()))
			require.Empty(t, r.reconcileUsed(t.Context()))
			f.mu.Lock()
			calls := f.contractCalls - before
			f.mu.Unlock()
			require.Equal(t, unresolved || tc.mined, calls > 0, "only unresolved or mined tickets need contract-state polling")
			if unresolved || tc.mined {
				f.mu.Lock()
				f.used[ticket.Hash()] = true
				f.receiptMissing = true
				f.mu.Unlock()
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
				require.Empty(t, r.reconcileUsed(t.Context()))
				require.Equal(t, "used", ticketPhase(t, s, ticket.Sig))
				var redeemed bool
				require.NoError(t, s.db.QueryRow("SELECT redeemed_at IS NOT NULL FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&redeemed))
				require.True(t, redeemed)
			}
		})
	}
}

// Configure before sending work so the callback is immutable while jobs run.
func captureRecoveryResults(t *testing.T, s *RedeemerDB) func() *eth.TransactionReceipt {
	t.Helper()
	results := make(chan *eth.TransactionReceipt, 8)
	completed := s.recovery.manager.Completed
	s.recovery.manager.Completed = func(ctx context.Context, result *eth.TransactionReceipt) { completed(ctx, result); results <- result }
	return func() *eth.TransactionReceipt {
		t.Helper()
		select {
		case result := <-results:
			return result
		case <-time.After(3 * time.Second):
			t.Fatal("redemption callback did not complete")
			return nil
		}
	}
}

func TestRedemptionCallbackObservations(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit_failure=%t", failCommit), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 112)
			s.metrics = observability.New("orchestrator")
			f.finalized = false
			f.mineAfter = 1
			if failCommit {
				_, err := s.db.Exec(`CREATE TRIGGER fail_mined BEFORE UPDATE OF phase ON redemption_attempts WHEN NEW.phase='mined' BEGIN SELECT RAISE(ABORT,'storage failure'); END`)
				require.NoError(t, err)
			}
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 0}))
			nextResult := captureRecoveryResults(t, s)
			tickRecovery(t, s, c, key)
			result := nextResult()
			require.NoError(t, result.Error())
			if failCommit {
				require.NotEqual(t, "mined", ticketPhase(t, s, ticket.Sig))
				require.Zero(t, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "mined"}))
				_, err := s.db.Exec(`DROP TRIGGER fail_mined`)
				require.NoError(t, err)
				require.NoError(t, s.recovery.recordResult(t.Context(), result))
			}
			require.Equal(t, "mined", ticketPhase(t, s, ticket.Sig))
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "mined"}))
			require.NoError(t, s.recovery.recordResult(t.Context(), result))
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "mined"}))
			require.Zero(t, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
			f.mu.Lock()
			f.finalized = true
			f.mu.Unlock()
			require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
			require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
		})
	}
}
