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
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

func TestRedemptionEligibility(t *testing.T) {
	for _, tc := range []struct {
		block, round int64
		active       bool
		lookupError  bool
	}{{9, 5, true, false}, {10, 5, true, false}, {11, 5, true, false}, {10, 5, false, false}, {10, 5, true, true}, {10, 8, true, false}} {
		t.Run(fmt.Sprintf("block=%d/round=%d/active=%t/error=%t", tc.block, tc.round, tc.active, tc.lookupError), func(t *testing.T) {
			path, passwordPath := test.WriteFixedKeystore(t)
			key, err := eth.OpenKeystoreFile(path, passwordPath)
			require.NoError(t, err)
			store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "payment.sqlite"))
			require.NoError(t, err)
			defer store.Close()
			ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: ethcommon.HexToAddress("0x1234"), Recipient: ethcommon.HexToAddress("0x5678"), FaceValue: big.NewInt(10), WinProb: big.NewInt(100), TicketNonce: 1, RecipientRandHash: ethcommon.HexToHash("0xabcd"), CreationRound: 5, ParamsExpirationBlock: big.NewInt(10)}, Sig: []byte{1, 2, 3}, RecipientRand: big.NewInt(4)}
			require.NoError(t, store.StoreWinningTicket(ticket))
			var broadcasts atomic.Int32
			rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Method string
					Params []json.RawMessage
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				var result any = "0x"
				switch req.Method {
				case "eth_call":
					result = "0x" + strings.Repeat("0", 60) + "2000"
					var call struct{ Input string }
					require.NoError(t, json.Unmarshal(req.Params[0], &call))
					selector := func(sig string) bool {
						return strings.HasPrefix(call.Input, fmt.Sprintf("0x%x", crypto.Keccak256([]byte(sig))[:4]))
					}
					switch {
					case selector("lastInitializedRound()"):
						result = fmt.Sprintf("0x%064x", tc.round)
					case selector("isActiveTranscoder(address)"):
						require.True(t, strings.HasSuffix(call.Input, fmt.Sprintf("%064x", ticket.Recipient.Bytes())))
						if tc.lookupError {
							require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32000, "message": "unavailable"}}))
							return
						}
						result = fmt.Sprintf("0x%064x", 0)
						if tc.active {
							result = fmt.Sprintf("0x%064x", 1)
						}
					}
				case "eth_getBlockByNumber":
					result = &types.Header{Number: big.NewInt(tc.block), Difficulty: new(big.Int), BaseFee: big.NewInt(1)}
				case "eth_estimateGas":
					result = "0x5208"
				case "eth_maxPriorityFeePerGas":
					result = "0x1"
				case "eth_getTransactionCount":
					result = "0x1"
				case "eth_sendRawTransaction":
					broadcasts.Add(1)
					var encoded string
					require.NoError(t, json.Unmarshal(req.Params[0], &encoded))
					raw, err := hex.DecodeString(strings.TrimPrefix(encoded, "0x"))
					require.NoError(t, err)
					var tx types.Transaction
					require.NoError(t, tx.UnmarshalBinary(raw))
					result = tx.Hash().Hex()
					// The exact transaction identity must exist before sending.
					items, err := store.Redemptions()
					require.NoError(t, err)
					require.Equal(t, []RedemptionStatus{{Hash: result.(string), Phase: "broadcast"}}, items)
				case "eth_getTransactionReceipt":
					result = nil
				default:
					t.Errorf("unexpected RPC method %s", req.Method)
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result}))
			}))
			defer rpcServer.Close()
			rpc, err := eth.OpenRPC(rpcServer.URL)
			require.NoError(t, err)
			contracts, err := eth.OpenContracts(rpc, "0x0000000000000000000000000000000000001000")
			require.NoError(t, err)
			failures := ProcessRedemptions(t.Context(), store, eth.PaymentChain{Contracts: contracts}, key, big.NewInt(1))
			require.Equal(t, tc.lookupError, len(failures) != 0, "%v", failures)
			expired := tc.round > 7
			redeemed := tc.block >= 10 && !expired && tc.active && !tc.lookupError
			require.Equal(t, redeemed, broadcasts.Load() == 1)
			queued, err := store.PendingPayers()
			require.NoError(t, err)
			require.Equal(t, !redeemed && !expired, len(queued) == 1)
		})
	}
}
