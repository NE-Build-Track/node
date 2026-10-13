package orchestrator

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

func TestRedemptionRecoveryBeforeAndAfterBroadcast(t *testing.T) {
	for _, failure := range []string{"preparation", "prepared-crash", "uncertain-broadcast"} {
		t.Run(failure, func(t *testing.T) {
			keyFile, passwordPath := test.WriteFixedKeystore(t)
			key, err := eth.OpenKeystoreFile(keyFile, passwordPath)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "payments.sqlite")
			store, err := OpenRedeemerDB(path)
			require.NoError(t, err)
			defer func() { _ = store.Close() }()
			ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: ethcommon.HexToAddress("0x1234"), Recipient: ethcommon.HexToAddress("0x5678"), FaceValue: big.NewInt(10), WinProb: big.NewInt(100), TicketNonce: 1, RecipientRandHash: ethcommon.HexToHash("0xabcd"), CreationRound: 1, ParamsExpirationBlock: big.NewInt(10)}, Sig: []byte{1, 2, 3}, RecipientRand: big.NewInt(4)}
			require.NoError(t, store.StoreWinningTicket(ticket))
			var fail atomic.Bool
			fail.Store(true)
			var snapshotUnavailable, validReceipt atomic.Bool
			var broadcasts atomic.Int32
			var firstRaw string
			receiptHeader := &types.Header{Number: big.NewInt(1), Difficulty: new(big.Int), BaseFee: big.NewInt(1)}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Method string
					Params []json.RawMessage
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				var result any = "0x1"
				switch req.Method {
				case "eth_call":
					// Reuse the 32-byte encoding of 1 for calls returning an address, round, hash, or bool.
					result = fmt.Sprintf("0x%064x", 1)
				case "eth_getBlockByNumber":
					if snapshotUnavailable.Load() && string(req.Params[0]) == `"latest"` {
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
						return
					}
					result = &types.Header{Number: big.NewInt(10), Difficulty: new(big.Int), BaseFee: big.NewInt(1)}
					if string(req.Params[0]) == `"0x1"` {
						result = receiptHeader
					}
				case "eth_estimateGas":
					if failure == "preparation" && fail.Load() {
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
						return
					}
					result = "0x5208"
				case "eth_getTransactionReceipt":
					result = map[string]any{"transactionHash": req.Params[0], "blockNumber": "0x1", "blockHash": receiptHeader.Hash(),
						"cumulativeGasUsed": "0x0", "gasUsed": "0x0", "logsBloom": types.Bloom{}, "logs": []any{}}
					if validReceipt.Load() {
						result.(map[string]any)["status"] = "0x1"
					}
				case "eth_maxPriorityFeePerGas", "eth_getTransactionCount":
				case "eth_sendRawTransaction":
					broadcasts.Add(1)
					var encoded string
					require.NoError(t, json.Unmarshal(req.Params[0], &encoded))
					if firstRaw != "" {
						require.Equal(t, firstRaw, encoded, "recovery must reuse the exact transaction")
					}
					firstRaw = encoded
					if failure == "uncertain-broadcast" && fail.Load() {
						http.Error(w, "lost response", http.StatusServiceUnavailable)
						return
					}
					raw, err := hex.DecodeString(strings.TrimPrefix(encoded, "0x"))
					require.NoError(t, err)
					var transaction types.Transaction
					require.NoError(t, transaction.UnmarshalBinary(raw))
					result = transaction.Hash().Hex()
				default:
					t.Errorf("unexpected RPC %s", req.Method)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
			}))
			defer server.Close()
			rpc, err := eth.OpenRPC(server.URL)
			require.NoError(t, err)
			contracts, err := eth.OpenContracts(rpc, ethcommon.HexToAddress("0x1000").Hex())
			require.NoError(t, err)
			chain := eth.PaymentChain{Contracts: contracts}
			snapshot, err := chain.Snapshot(t.Context())
			require.NoError(t, err)
			if failure == "prepared-crash" {
				prepared, err := chain.PrepareRedemption(t.Context(), key, big.NewInt(1), eth.RedeemTicket{Recipient: ticket.Recipient, Sender: ticket.PayerAddress, FaceValue: ticket.FaceValue, WinProb: ticket.WinProb, SenderNonce: ticket.TicketNonce, RecipientRandHash: ticket.RecipientRandHash, AuxData: ticket.AuxData(), Signature: ticket.Sig, RecipientRand: ticket.RecipientRand})
				require.NoError(t, err)
				firstRaw = "0x" + hex.EncodeToString(prepared.Raw)
				require.NoError(t, store.recordPrepared(t.Context(), ticket, prepared))
			} else {
				require.NotEmpty(t, RedeemPending(t.Context(), store, chain, key, big.NewInt(1), snapshot))
			}
			require.NoError(t, store.Close())
			store, err = OpenRedeemerDB(path)
			require.NoError(t, err)
			fail.Store(false)
			restartKey := key
			if failure == "prepared-crash" {
				restartKey = paymentTestKey(t)
			}
			require.Empty(t, RedeemPending(t.Context(), store, chain, restartKey, big.NewInt(1), snapshot))
			require.Equal(t, int32(1), broadcasts.Load(), "uncertain sends cannot retry automatically")
			items, err := store.Redemptions()
			require.NoError(t, err)
			require.Len(t, items, 1)
			if failure == "uncertain-broadcast" {
				require.Equal(t, "broadcast", items[0].Phase)
				require.NoError(t, RetryRedemption(t.Context(), store, contracts, ethcommon.HexToHash(items[0].Hash)))
				require.Equal(t, int32(2), broadcasts.Load())
			}
			before, err := store.Redemptions()
			require.NoError(t, err)
			require.NotEmpty(t, ReconcileSubmitted(t.Context(), store, chain), "missing receipt status must fail reconciliation")
			after, err := store.Redemptions()
			require.NoError(t, err)
			require.Equal(t, before, after, "malformed receipts must leave durable attempts unresolved")
			next := *ticket
			next.Ticket = &pm.Ticket{PayerAddress: ticket.PayerAddress, Recipient: ticket.Recipient, FaceValue: ticket.FaceValue, WinProb: ticket.WinProb, TicketNonce: 2, RecipientRandHash: ticket.RecipientRandHash, CreationRound: ticket.CreationRound, ParamsExpirationBlock: big.NewInt(10)}
			next.Sig = []byte{4, 5, 6}
			require.NoError(t, store.StoreWinningTicket(&next))
			validReceipt.Store(true)
			snapshotUnavailable.Store(true)
			sent := broadcasts.Load()
			failures := ProcessRedemptions(t.Context(), store, chain, restartKey, big.NewInt(1))
			require.NotEmpty(t, failures)
			after, err = store.Redemptions()
			require.NoError(t, err)
			require.Equal(t, "confirmed", after[0].Phase, "the worker must reconcile despite an unavailable snapshot")
			require.Equal(t, sent, broadcasts.Load())
			queued, err := store.PendingPayers()
			require.NoError(t, err)
			require.Equal(t, []ethcommon.Address{ticket.PayerAddress}, queued)
			require.Error(t, RetryRedemption(t.Context(), store, contracts, ethcommon.HexToHash(items[0].Hash)))
			if failure == "prepared-crash" {
				floor, err := store.SavedNonceFloor(t.Context(), key.Address())
				require.NoError(t, err)
				require.EqualValues(t, 2, floor)
				floor, err = store.SavedNonceFloor(t.Context(), restartKey.Address())
				require.NoError(t, err)
				require.Zero(t, floor)
				snapshotUnavailable.Store(false)
				// A fresh client sees the same stale RPC nonce after restart.
				// Saved, confirmed identities must still reserve their nonce.
				require.NoError(t, store.Close())
				store, err = OpenRedeemerDB(path)
				require.NoError(t, err)
				contracts, err = eth.OpenContracts(rpc, ethcommon.HexToAddress("0x1000").Hex())
				require.NoError(t, err)
				chain = eth.PaymentChain{Contracts: contracts}
				firstRaw = ""
				require.Empty(t, RedeemPending(t.Context(), store, chain, key, big.NewInt(1), snapshot))
				encoded, err := hex.DecodeString(strings.TrimPrefix(firstRaw, "0x"))
				require.NoError(t, err)
				var signed types.Transaction
				require.NoError(t, signed.UnmarshalBinary(encoded))
				require.EqualValues(t, 2, signed.Nonce(), "confirmed history must advance a fresh client's nonce floor")
			}
		})
	}
}
