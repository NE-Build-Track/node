package eth

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestRedemptionSubmission(t *testing.T) {
	keyPath, passwordPath := test.WriteFixedKeystore(t)
	key, err := OpenKeystoreFile(keyPath, passwordPath)
	require.NoError(t, err)
	controller, broker, sender := common.HexToAddress("0x1000"), common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
	auxData, err := hexutil.Decode("0x" + word(5) + common.HexToHash("0xabcd").Hex()[2:])
	require.NoError(t, err)
	ticket := RedeemTicket{Recipient: common.HexToAddress("0x5000"), Sender: sender, FaceValue: big.NewInt(10), WinProb: big.NewInt(100), SenderNonce: 4294967295, RecipientRandHash: common.HexToHash("0x4000"), AuxData: auxData, Signature: []byte{1, 2, 3}, RecipientRand: big.NewInt(4)}
	// Independent encoding checks tuple fields, sender-nonce widening, and the
	// offsets and contents of auxiliary data and the signature.
	wantData := calldata("redeemWinningTicket((address,address,uint256,uint256,uint256,bytes32,bytes),bytes,uint256)",
		word(96), word(416), word(4), addressWord(ticket.Recipient), addressWord(sender), word(10), word(100), word(4294967295),
		ticket.RecipientRandHash.Hex()[2:], word(224), word(64), word(5), common.HexToHash("0xabcd").Hex()[2:], word(3), "010203"+strings.Repeat("0", 58))
	var sentHash common.Hash
	rpc := testRPC(t, func(method string, params []json.RawMessage) any {
		var result any
		switch method {
		case "eth_call":
			var call struct {
				From, To common.Address
				Input    string
			}
			require.NoError(t, json.Unmarshal(params[0], &call))
			if call.To == controller {
				require.Equal(t, calldata("getContract(bytes32)", hexutil.Encode(crypto.Keccak256([]byte("TicketBroker")))[2:]), call.Input)
				result = "0x" + addressWord(broker)
			} else {
				require.JSONEq(t, `"pending"`, string(params[1]))
				require.Equal(t, key.Address(), call.From)
				require.Equal(t, broker, call.To)
				require.Equal(t, wantData, call.Input)
				result = "0x"
			}
		case "eth_getBlockByNumber":
			result = &types.Header{Number: big.NewInt(1), Difficulty: new(big.Int), BaseFee: big.NewInt(1)}
		case "eth_estimateGas":
			result = "0x5208"
		case "eth_maxPriorityFeePerGas", "eth_getTransactionCount":
			result = "0x1"
		case "eth_sendRawTransaction":
			var encoded string
			require.NoError(t, json.Unmarshal(params[0], &encoded))
			raw, err := hexutil.Decode(encoded)
			require.NoError(t, err)
			var tx types.Transaction
			require.NoError(t, tx.UnmarshalBinary(raw))
			require.Equal(t, wantData, hexutil.Encode(tx.Data()))
			from, err := types.Sender(types.LatestSignerForChainID(big.NewInt(1)), &tx)
			require.NoError(t, err)
			require.Equal(t, key.Address(), from)
			sentHash = tx.Hash()
			result = sentHash
		default:
			t.Errorf("unexpected RPC %s", method)
		}
		return result
	})
	contracts, err := NewContracts(rpc, controller)
	require.NoError(t, err)
	hash, err := (PaymentChain{Contracts: contracts}).Redeem(t.Context(), key, big.NewInt(1), ticket)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, hash)
	require.Equal(t, sentHash, hash)
}
