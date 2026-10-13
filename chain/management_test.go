package chain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/eth/contracts"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

type managementFixture struct {
	mu                            sync.Mutex
	server                        *httptest.Server
	addresses                     map[string]common.Address
	abis                          map[common.Address]*abi.ABI
	reads                         map[string][]any
	readHook                      func(string, []any) []any
	calls, simulations            []string
	sent                          []*types.Transaction
	receipts                      int
	estimates                     int
	snapshots                     int
	failSimulation, failReceipt   string
	receiptHook                   func(common.Hash) (any, bool)
	requireSnapshot, noncanonical bool
	failRPC                       string
	simulationHook                func(common.Address, common.Address, []byte, string)
	account                       common.Address
}

func newManagementFixture(t *testing.T) *managementFixture {
	t.Helper()
	f := &managementFixture{addresses: map[string]common.Address{}, abis: map[common.Address]*abi.ABI{}, account: common.HexToAddress(testAccount)}
	metadata := map[string]*bind.MetaData{"Controller": contracts.ControllerMetaData, "BondingManager": contracts.BondingManagerMetaData, "RoundsManager": contracts.RoundsManagerMetaData, "TicketBroker": contracts.TicketBrokerMetaData, "ServiceRegistry": contracts.ServiceRegistryMetaData, "LivepeerToken": contracts.LivepeerTokenMetaData, "Minter": contracts.MinterMetaData, "LivepeerGovernor": contracts.GovernorMetaData, "Poll": contracts.PollMetaData}
	for i, name := range []string{"Controller", "BondingManager", "RoundsManager", "TicketBroker", "ServiceRegistry", "LivepeerToken", "Minter", "LivepeerGovernor", "Poll"} {
		address := common.BigToAddress(big.NewInt(int64(i + 1)))
		if name == "Controller" {
			address = common.HexToAddress("0xD8E8328501E9645d16Cf49539efC04f734606ee4")
		}
		f.addresses[name] = address
		parsed, err := metadata[name].GetAbi()
		require.NoError(t, err)
		f.abis[address] = parsed
	}
	n := func(value int64) *big.Int { return big.NewInt(value) }
	f.reads = map[string][]any{
		"balanceOf": {n(50)}, "allowance": {n(50)}, "getDelegator": {n(20), n(2), f.account, n(22), n(5), n(4), n(5)}, "delegatorStatus": {uint8(1)}, "pendingStake": {n(25)}, "pendingFees": {n(7)},
		"getTranscoderPoolMaxSize": {n(10)}, "getFirstTranscoderInPool": {common.Address{}}, "getNextTranscoderInPool": {common.Address{}}, "transcoderTotalStake": {n(22)}, "isActiveTranscoder": {true}, "isRegisteredTranscoder": {true}, "transcoderToRewardCaller": {common.Address{}},
		"getTranscoder": {n(4), n(10000), n(980000), n(4), n(1), n(1000), n(0), n(0), n(0), n(4)}, "getServiceURI": {"https://orchestrator.example"},
		"getDelegatorUnbondingLock": {n(5), n(10)}, "getTranscoderEarningsPoolForRound": {n(20), n(0), n(0), n(0), n(0)}, "getTotalBonded": {n(100)}, "currentMintableTokens": {n(10)},
		"currentRound": {n(10)}, "lastInitializedRound": {n(10)}, "currentRoundInitialized": {true}, "currentRoundLocked": {false}, "roundLength": {n(50)}, "roundLockAmount": {n(100000)}, "unbondingPeriod": {uint64(7)},
		"paused": {false}, "inflation": {n(100)}, "inflationChange": {n(10)}, "targetBondingRate": {n(500000)}, "getGlobalTotalSupply": {n(200)},
		"getSenderInfo": {contracts.MixinTicketBrokerCoreSender{Deposit: n(5), WithdrawRound: n(0)}, contracts.MReserveReserveInfo{FundsRemaining: n(8), ClaimedInCurrentRound: n(0)}}, "unlockPeriod": {n(3)},
	}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var request struct {
			Method string
			Params []json.RawMessage
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		var result any
		switch request.Method {
		case "eth_chainId":
			result = "0xa4b1"
		case "eth_getBlockByNumber":
			f.snapshots++
			header := &types.Header{Number: big.NewInt(16), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}
			result = header
		case "eth_getBalance":
			f.assertSnapshot(t, request.Params[1])
			result = "0xde0b6b3a7640000"
		case "eth_getTransactionCount":
			require.JSONEq(t, `"pending"`, string(request.Params[1]))
			result = hexutil.EncodeUint64(uint64(len(f.sent)))
		case "eth_maxPriorityFeePerGas":
			result = "0x3"
		case "eth_estimateGas":
			f.estimates++
			result = hexutil.EncodeUint64(uint64(21000 + f.estimates))
		case "eth_call":
			var input struct {
				From  common.Address
				Value string
				To    common.Address
				Data  hexutil.Bytes `json:"input"`
			}
			require.NoError(t, json.Unmarshal(request.Params[0], &input))
			if f.noncanonical {
				f.assertSnapshot(t, request.Params[1])
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32000, "message": "block is not canonical"}}))
				return
			}
			contract := f.abis[input.To]
			require.NotNil(t, contract)
			method, err := contract.MethodById(input.Data)
			require.NoError(t, err)
			args, err := method.Inputs.Unpack(input.Data[4:])
			require.NoError(t, err)
			f.calls = append(f.calls, method.Name)
			if method.Name == "getContract" {
				if f.requireSnapshot || string(request.Params[1]) != `"latest"` {
					f.assertSnapshot(t, request.Params[1])
				}
				var address common.Address
				for name, candidate := range f.addresses {
					if crypto.Keccak256Hash([]byte(name)) == common.Hash(args[0].([32]byte)) {
						address = candidate
					}
				}
				require.NotEqual(t, common.Address{}, address)
				data, err := method.Outputs.Pack(address)
				require.NoError(t, err)
				result = hexutil.Encode(data)
			} else if method.StateMutability == "view" || method.StateMutability == "pure" {
				if f.requireSnapshot || string(request.Params[1]) != `"latest"` {
					f.assertSnapshot(t, request.Params[1])
				}
				values := f.reads[method.Name]
				if f.readHook != nil {
					if override := f.readHook(method.Name, args); override != nil {
						values = override
					}
				}
				require.NotNil(t, values, "missing fixture read %s", method.Name)
				data, err := method.Outputs.Pack(values...)
				require.NoError(t, err)
				result = hexutil.Encode(data)
			} else {
				require.JSONEq(t, `"pending"`, string(request.Params[1]))
				f.simulations = append(f.simulations, method.Name)
				if f.simulationHook != nil {
					f.simulationHook(input.From, input.To, input.Data, input.Value)
				}
				if f.failSimulation == method.Name {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				// All staking hints must be prepared from the already confirmed state.
				if method.Name == "bondWithHint" {
					require.GreaterOrEqual(t, f.reads["allowance"][0].(*big.Int).Cmp(args[0].(*big.Int)), 0)
				}
				if method.Name == "rebondWithHint" {
					require.Equal(t, f.account, f.reads["getDelegator"][2])
				}
				result = "0x"
			}
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			require.NoError(t, json.Unmarshal(request.Params[0], &raw))
			tx := new(types.Transaction)
			require.NoError(t, tx.UnmarshalBinary(raw))
			f.sent = append(f.sent, tx)
			result = tx.Hash().Hex()
		case "eth_getTransactionReceipt":
			f.receipts++
			var requested common.Hash
			require.NoError(t, json.Unmarshal(request.Params[0], &requested))
			if f.receiptHook != nil {
				if override, ok := f.receiptHook(requested); ok {
					result = override
					break
				}
			}
			last := f.sent[len(f.sent)-1]
			method, err := f.abis[*last.To()].MethodById(last.Data())
			require.NoError(t, err)
			args, err := method.Inputs.Unpack(last.Data()[4:])
			require.NoError(t, err)
			status := uint64(1)
			if f.failReceipt == method.Name {
				status = 0
			}
			if status == 1 {
				switch method.Name {
				case "approve":
					f.reads["allowance"] = []any{args[1]}
				case "bondWithHint":
					f.reads["getDelegator"][2] = args[1]
				case "rebondFromUnbondedWithHint":
					f.reads["getDelegator"][2] = args[0]
				}
			}
			result = &types.Receipt{TxHash: last.Hash(), Status: status, BlockNumber: big.NewInt(16), BlockHash: common.Hash{31: 1}, Logs: []*types.Log{}}
		default:
			t.Errorf("unexpected RPC %s", request.Method)
		}
		if f.failRPC == request.Method {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result}))
	}))
	t.Cleanup(f.server.Close)
	return f
}
func (f *managementFixture) assertSnapshot(t *testing.T, raw json.RawMessage) {
	t.Helper()
	header := &types.Header{Number: big.NewInt(16), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}
	require.JSONEq(t, fmt.Sprintf(`{"blockHash":%q,"requireCanonical":true}`, header.Hash().Hex()), string(raw))
}
func (f *managementFixture) run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	t.Setenv("LIVEPEER_CHAIN_RPC_URL", f.server.URL)
	var output bytes.Buffer
	root := Root(&output, &output)
	root.SetArgs(append(args, "--account", f.account.Hex(), "--output", "json"))
	err := root.Execute()
	return output.String(), err
}
func decodeObject(t *testing.T, text string) map[string]any {
	t.Helper()
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(text), &result))
	return result
}

func TestInspectionCommands(t *testing.T) {
	for _, tc := range []struct {
		command string
		fields  map[string]any
	}{
		{"account get", map[string]any{"balance_wei": "1000000000000000000", "lpt_balance_base_units": "50", "pending_nonce": float64(0)}},
		{"stake get", map[string]any{"status": "Bonded", "bonded_stake_base_units": "20", "pending_stake_base_units": "25", "collected_fees_wei": "2", "pending_fees_wei": "7", "delegated_amount_base_units": "22", "start_round": "5", "last_claim_round": "4", "next_lock_id": "5"}},
		{"orchestrator get", map[string]any{"registered": true, "active": true, "delegated_stake_base_units": "22", "reward_cut_percent": "1", "fee_cut_percent": "2", "last_reward_round": "4", "service_uri": "https://orchestrator.example"}},
		{"round get", map[string]any{"current_round": "10", "last_initialized_round": "10", "initialized": true, "locked": false}},
		{"protocol get", map[string]any{"paused": false, "pool_limit": "10", "round_length": "50", "round_lock_amount": "100000", "unbonding_period": "7", "inflation": "100", "inflation_change": "10", "target_bonding_rate": "500000", "supply_base_units": "200", "total_bonded_base_units": "100", "participation_rate_percent": "50"}},
		{"ticketbroker get", map[string]any{"deposit_wei": "5", "reserve_wei": "8", "withdraw_round": "0", "unlock_period": "3", "funding_status": "Locked", "projected_withdraw_round": "13"}},
		{"orchestrator reward-caller get", map[string]any{"reward_caller": strings.ToLower(common.Address{}.Hex())}},
	} {
		t.Run(tc.command, func(t *testing.T) {
			f := newManagementFixture(t)
			f.requireSnapshot = true
			out, err := f.run(t, strings.Fields(tc.command)...)
			require.NoError(t, err)
			object := decodeObject(t, out)
			for key, value := range tc.fields {
				require.Equal(t, value, object[key], key)
			}
			require.Equal(t, 1, f.snapshots)
			require.NotContains(t, f.calls, "getDelegatorUnbondingLock")
		})
	}
	t.Run("addresses", func(t *testing.T) {
		f := newManagementFixture(t)
		f.requireSnapshot = true
		out, err := f.run(t, "contracts")
		require.NoError(t, err)
		require.Len(t, decodeObject(t, out)["addresses"], 8)
		require.Equal(t, 1, f.snapshots)
	})
	t.Run("gas", func(t *testing.T) {
		f := newManagementFixture(t)
		f.requireSnapshot = true
		out, err := f.run(t, "gas", "get", "--max-priority-fee-per-gas", "10", "--max-fee-per-gas", "210")
		require.NoError(t, err)
		object := decodeObject(t, out)
		require.Equal(t, "210", object["calculated_fee_cap_wei"])
		require.Equal(t, "3", object["suggested_priority_fee_per_gas_wei"])
		require.Equal(t, "10", object["max_priority_fee_per_gas_wei"])
	})
}
func TestLocksPaginationAndFilters(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		ids     []string
		next    any
		queries int
	}{
		{"page", []string{"--limit", "3"}, []string{"0", "2"}, "3", 3},
		{"withdrawable", []string{"--limit", "3", "--withdrawable", "--locked=false"}, []string{"0"}, "3", 3},
		{"locked", []string{"--from-id", "1", "--limit", "3", "--locked"}, []string{"2"}, "4", 3},
		{"end", []string{"--from-id", "4"}, []string{"4"}, nil, 1},
		{"beyond", []string{"--from-id", "8"}, nil, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newManagementFixture(t)
			f.requireSnapshot = true
			queried := 0
			f.readHook = func(name string, args []any) []any {
				if name == "getDelegatorUnbondingLock" {
					queried++
					id := args[1].(*big.Int).Uint64()
					round := int64(0)
					switch id {
					case 0:
						round = 10
					case 2:
						round = 11
					case 4:
						round = 9
					}
					return []any{big.NewInt(3), big.NewInt(round)}
				}
				return nil
			}
			out, err := f.run(t, append([]string{"stake", "locks"}, tc.args...)...)
			require.NoError(t, err)
			object := decodeObject(t, out)
			var ids []string
			for _, lock := range object["locks"].([]any) {
				ids = append(ids, lock.(map[string]any)["id"].(string))
			}
			require.Equal(t, tc.ids, ids)
			require.Equal(t, tc.next, object["next_from_id"])
			require.Equal(t, tc.queries, queried)
			require.Equal(t, 1, f.snapshots)
		})
	}
	f := newManagementFixture(t)
	f.requireSnapshot = true
	_, err := f.run(t, "stake", "locks", "--locked", "--withdrawable")
	require.ErrorContains(t, err, "mutually exclusive")
}
func TestPoolAndEmptyInspection(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			f := newManagementFixture(t)
			f.requireSnapshot = true
			a, b := common.HexToAddress("0x100"), common.HexToAddress("0x200")
			f.reads["getFirstTranscoderInPool"] = []any{a}
			f.readHook = func(name string, args []any) []any {
				if name == "getNextTranscoderInPool" {
					if args[0].(common.Address) == a {
						return []any{b}
					}
					return []any{common.Address{}}
				}
				if name == "isActiveTranscoder" {
					return []any{args[0].(common.Address) == a}
				}
				return nil
			}
			args := []string{"orchestrator", "list"}
			if active {
				args = append(args, "--active")
			}
			out, err := f.run(t, args...)
			require.NoError(t, err)
			size := 2
			if active {
				size = 1
			}
			require.Len(t, decodeObject(t, out)["orchestrators"], size)
			require.Equal(t, 1, f.snapshots)
		})
	}
	t.Run("empty pool", func(t *testing.T) {
		f := newManagementFixture(t)
		f.requireSnapshot = true
		out, err := f.run(t, "orchestrator", "list")
		require.NoError(t, err)
		require.Empty(t, decodeObject(t, out)["orchestrators"])
	})
	for _, tc := range []struct {
		round  int64
		empty  bool
		status string
	}{{0, true, "Empty"}, {11, false, "Unlocking"}, {10, false, "Unlocked"}} {
		t.Run(tc.status, func(t *testing.T) {
			f := newManagementFixture(t)
			f.requireSnapshot = true
			deposit := big.NewInt(5)
			if tc.empty {
				deposit.SetInt64(0)
			}
			f.reads["getSenderInfo"] = []any{contracts.MixinTicketBrokerCoreSender{Deposit: deposit, WithdrawRound: big.NewInt(tc.round)}, contracts.MReserveReserveInfo{FundsRemaining: new(big.Int), ClaimedInCurrentRound: new(big.Int)}}
			out, err := f.run(t, "ticketbroker", "get")
			require.NoError(t, err)
			require.Equal(t, tc.status, decodeObject(t, out)["funding_status"])
		})
	}
}

func TestResolvedAmountsAndCalldata(t *testing.T) {
	for _, tc := range []struct {
		command   string
		signature string
		words     []string
	}{
		{"stake unbond --amount all", "unbondWithHint(uint256,address,address)", []string{uintWord(25), uintWord(0), uintWord(0)}},
		{"stake unbond --amount 0.000000000000000005", "unbondWithHint(uint256,address,address)", []string{uintWord(5), uintWord(0), uintWord(0)}},
		{"earnings withdraw-fees --amount all", "withdrawFees(address,uint256)", []string{addressWord(testAccount), uintWord(7)}},
		{"token transfer " + testAccount + " --amount all", "transfer(address,uint256)", []string{addressWord(testAccount), uintWord(50)}},
		{"token transfer " + testAccount + " --amount 5 --base-units", "transfer(address,uint256)", []string{addressWord(testAccount), uintWord(5)}},
		{"stake bond " + testAccount + " --amount all", "bondWithHint(uint256,address,address,address,address,address)", []string{uintWord(50), addressWord(testAccount), uintWord(0), uintWord(0), uintWord(0), uintWord(0)}},
		{"earnings claim", "claimEarnings(uint256)", []string{uintWord(10)}},
		{"governance poll vote 0x0000000000000000000000000000000000000009 no", "vote(uint256)", []string{uintWord(1)}},
		{"governance proposal vote 42 abstain", "castVote(uint256,uint8)", []string{uintWord(42), uintWord(2)}},
		{"stake withdraw --lock-id 0", "withdrawStake(uint256)", []string{uintWord(0)}},
		{"ticketbroker fund --amount 0.000000000000000001 --reserve 0.000000000000000002", "fundDepositAndReserve(uint256,uint256)", []string{uintWord(1), uintWord(2)}},
	} {
		t.Run(tc.command, func(t *testing.T) {
			f := newManagementFixture(t)
			f.reads["transcoderTotalStake"] = []any{big.NewInt(50)}
			out, err := f.run(t, strings.Fields(tc.command)...)
			require.NoError(t, err)
			record := decodeObject(t, out)
			simulation := record["simulation"].(map[string]any)
			require.Equal(t, calldata(tc.signature, tc.words...), simulation["data"])
			require.Empty(t, f.sent)
			require.Equal(t, 1, f.estimates)
		})
	}
	f := newManagementFixture(t)
	out, err := f.run(t, "governance", "proposal", "vote", "7", "for", "--reason", "useful reason")
	require.NoError(t, err)
	require.Equal(t, calldata("castVoteWithReason(uint256,uint8,string)", uintWord(7), uintWord(1), uintWord(96), uintWord(13), hexutil.Encode([]byte("useful reason"))[2:]+strings.Repeat("0", 38)), decodeObject(t, out)["simulation"].(map[string]any)["data"])
}
func TestCancellationModes(t *testing.T) {
	other := common.HexToAddress("0x1234")
	for _, tc := range []struct {
		status          uint8
		address         string
		method, wantErr string
	}{
		{0, "", "rebondWithHint", ""}, {1, "", "rebondWithHint", ""}, {1, testAccount, "rebondWithHint", ""}, {1, other.Hex(), "", "does not match"},
		{2, "", "", "address is required"}, {2, testAccount, "rebondFromUnbondedWithHint", ""},
	} {
		t.Run(fmt.Sprintf("%d/%s", tc.status, tc.address), func(t *testing.T) {
			f := newManagementFixture(t)
			f.reads["delegatorStatus"] = []any{tc.status}
			args := []string{"stake", "cancel-unbond"}
			if tc.address != "" {
				args = append(args, tc.address)
			}
			args = append(args, "--lock-id", "0")
			_, err := f.run(t, args...)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Empty(t, f.simulations)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{tc.method}, f.simulations)
			}
		})
	}
}
func TestRegistrationSequence(t *testing.T) {
	for _, tc := range []struct {
		name              string
		mode              []string
		unbonded, self    bool
		submit            bool
		fail, expectedErr string
		methods           []string
		sent              int
	}{
		{"missing staking mode", nil, false, false, false, "", "requires --amount", nil, 0},
		{"optional URI", []string{"--redelegate"}, false, false, true, "", "", []string{"bondWithHint", "transcoder"}, 2},
		{"unchanged commissions", []string{"--redelegate"}, false, false, true, "", "", []string{"bondWithHint"}, 1},
		{"locked round", []string{"--amount", "5", "--base-units"}, true, false, true, "", "round is locked", nil, 0},
		{"lock preview", []string{"--lock-id", "0"}, false, false, false, "", "", []string{"bondWithHint"}, 0},
		{"lock submission", []string{"--lock-id", "0"}, false, false, true, "", "", []string{"bondWithHint", "rebondWithHint", "transcoder"}, 3},
		{"unbonded lock", []string{"--lock-id", "0"}, true, false, true, "", "", []string{"rebondFromUnbondedWithHint", "transcoder"}, 2},
		{"partial failure", []string{"--lock-id", "0"}, false, false, true, "rebondWithHint", "step 2 of 3", []string{"bondWithHint", "rebondWithHint"}, 1},
		{"approval then registration", []string{"--amount", "5", "--base-units"}, true, false, true, "", "", []string{"approve", "bondWithHint", "transcoder"}, 3},
		{"existing self registration", nil, false, true, false, "", "", nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newManagementFixture(t)
			keyPath, passwordPath := test.WriteFixedKeystore(t)
			key, err := eth.OpenKeystoreFile(keyPath, passwordPath)
			require.NoError(t, err)
			f.account = key.Address()
			f.reads["transcoderTotalStake"] = []any{big.NewInt(50)}
			f.reads["getDelegator"][2] = common.HexToAddress("0x1234")
			if tc.self {
				f.reads["getDelegator"][2] = f.account
			}
			if tc.unbonded {
				f.reads["delegatorStatus"] = []any{uint8(2)}
			}
			f.reads["getServiceURI"] = []any{""}
			f.reads["currentRoundLocked"] = []any{tc.name == "locked round" || tc.name == "unchanged commissions" || tc.self}
			if strings.Contains(tc.name, "approval") {
				f.reads["allowance"] = []any{new(big.Int)}
			}
			f.failSimulation = tc.fail
			rewardCut := "3"
			if tc.self || tc.name == "unchanged commissions" {
				rewardCut = "1"
			}
			args := append([]string{"orchestrator", "register", "--reward-cut", rewardCut, "--fee-cut", "2"}, tc.mode...)
			if tc.submit {
				args = append(args, "--submit", "--keystore-file", keyPath, "--keystore-password-file", passwordPath)
			}
			if tc.fail != "" {
				args = append(args, "--quiet")
			}
			out, err := f.run(t, args...)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				for _, tx := range f.sent {
					require.ErrorContains(t, err, tx.Hash().Hex())
				}
				require.Empty(t, out)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.methods, f.simulations)
			require.NotContains(t, f.calls, "getServiceURI", "an omitted URI needs no registry lookup")
			require.Len(t, f.sent, tc.sent)
			require.Equal(t, tc.sent, f.receipts)
			if tc.name == "lock preview" {
				require.Contains(t, out, "deferred until preceding transactions confirm")
				require.Contains(t, out, `"steps":3`)
				require.Contains(t, out, `"method":"rebond"`)
				require.Contains(t, out, `"method":"transcoder"`)
			}
		})
	}
}
func TestNoOpsAndPrerequisiteErrors(t *testing.T) {
	for _, command := range []string{"orchestrator set-config --reward-cut 1 --fee-cut 2 --service-uri https://orchestrator.example", "orchestrator reward-caller unset", "stake bond " + testAccount + " --redelegate"} {
		t.Run(command, func(t *testing.T) {
			f := newManagementFixture(t)
			f.reads["currentRoundLocked"] = []any{true}
			out, err := f.run(t, strings.Fields(command)...)
			require.NoError(t, err)
			require.Equal(t, true, decodeObject(t, out)["no_op"])
			require.Empty(t, f.simulations)
		})
	}
	for _, tc := range []struct {
		command, field string
		value          []any
		error          string
	}{
		{"token transfer " + testAccount + " --amount all", "balanceOf", []any{new(big.Int)}, "zero"},
		{"stake bond " + testAccount + " --amount all", "balanceOf", []any{new(big.Int)}, "zero"},
		{"stake unbond --amount all", "pendingStake", []any{new(big.Int)}, "zero"},
		{"earnings withdraw-fees --amount all", "pendingFees", []any{new(big.Int)}, "zero"},
		{"earnings claim", "currentRoundInitialized", []any{false}, "not initialized"},
		{"reward call", "isActiveTranscoder", []any{false}, "not active"},
		{"stake bond " + testAccount + " --redelegate", "delegatorStatus", []any{uint8(2)}, "requires existing stake"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			f := newManagementFixture(t)
			f.reads[tc.field] = tc.value
			_, err := f.run(t, strings.Fields(tc.command)...)
			require.ErrorContains(t, err, tc.error)
			require.Empty(t, f.simulations)
		})
	}
	f := newManagementFixture(t)
	f.failSimulation = "rewardForTranscoderWithHint"
	_, err := f.run(t, "reward", "call", "--orchestrator", "0x0000000000000000000000000000000000001234")
	require.ErrorContains(t, err, "simulation failed")
}

func TestTransactionWaitingOptions(t *testing.T) {
	t.Run("broadcast only", func(t *testing.T) {
		f := newManagementFixture(t)
		keyPath, passwordPath := test.WriteFixedKeystore(t)
		key, err := eth.OpenKeystoreFile(keyPath, passwordPath)
		require.NoError(t, err)
		f.account = key.Address()
		_, err = f.run(t, "ticketbroker", "unlock", "--submit", "--no-wait", "--keystore-file", keyPath, "--keystore-password-file", passwordPath)
		require.NoError(t, err)
		require.Len(t, f.sent, 1)
		require.Zero(t, f.receipts)
	})
	t.Run("replacement hash reporting", func(t *testing.T) {
		f := newManagementFixture(t)
		keyPath, passwordPath := test.WriteFixedKeystore(t)
		key, err := eth.OpenKeystoreFile(keyPath, passwordPath)
		require.NoError(t, err)
		f.account = key.Address()
		f.receiptHook = func(common.Hash) (any, bool) { return nil, true }
		_, err = f.run(t, "ticketbroker", "unlock", "--submit", "--quiet", "--transaction-timeout", "50ms", "--max-transaction-replacements", "1", "--keystore-file", keyPath, "--keystore-password-file", passwordPath)
		require.ErrorContains(t, err, "2 attempt(s)")
		require.Len(t, f.sent, 2)
		for _, attempt := range f.sent {
			require.ErrorContains(t, err, attempt.Hash().Hex())
		}
	})
}

func TestConfigurationWritesAndEmptyStates(t *testing.T) {
	t.Run("single commission preserves other", func(t *testing.T) {
		f := newManagementFixture(t)
		out, err := f.run(t, "orchestrator", "set-config", "--fee-cut", "1.2345")
		require.NoError(t, err)
		require.Equal(t, calldata("transcoder(uint256,uint256)", uintWord(10000), uintWord(987655)), decodeObject(t, out)["simulation"].(map[string]any)["data"])
		require.Equal(t, []string{"transcoder"}, f.simulations)
	})
	t.Run("URI-only update during locked round", func(t *testing.T) {
		f := newManagementFixture(t)
		f.reads["currentRoundLocked"] = []any{true}
		const uri = "https://new-orchestrator.example"
		out, err := f.run(t, "orchestrator", "set-config", "--service-uri", uri)
		require.NoError(t, err)
		require.Equal(t, []string{"setServiceURI"}, f.simulations)
		data := decodeObject(t, out)["simulation"].(map[string]any)["data"].(string)
		method := f.abis[f.addresses["ServiceRegistry"]].Methods["setServiceURI"]
		args, err := method.Inputs.Unpack(hexutil.MustDecode(data)[4:])
		require.NoError(t, err)
		require.Equal(t, []any{uri}, args)
	})
	t.Run("reward caller write and no-op", func(t *testing.T) {
		f := newManagementFixture(t)
		target := common.HexToAddress("0x1234")
		out, err := f.run(t, "orchestrator", "reward-caller", "set", target.Hex())
		require.NoError(t, err)
		require.Equal(t, calldata("setRewardCaller(address)", addressWord(target.Hex())), decodeObject(t, out)["simulation"].(map[string]any)["data"])
		f.reads["transcoderToRewardCaller"] = []any{target}
		out, err = f.run(t, "orchestrator", "reward-caller", "set", target.Hex())
		require.NoError(t, err)
		require.Equal(t, true, decodeObject(t, out)["no_op"])
		require.Len(t, f.simulations, 1)
	})
	t.Run("zero stake", func(t *testing.T) {
		f := newManagementFixture(t)
		f.reads["getDelegator"] = []any{new(big.Int), new(big.Int), common.Address{}, new(big.Int), new(big.Int), new(big.Int), new(big.Int)}
		f.reads["delegatorStatus"] = []any{uint8(2)}
		f.reads["pendingStake"] = []any{new(big.Int)}
		f.reads["pendingFees"] = []any{new(big.Int)}
		out, err := f.run(t, "stake", "get")
		require.NoError(t, err)
		object := decodeObject(t, out)
		require.Equal(t, "Unbonded", object["status"])
		require.Equal(t, "0", object["pending_stake_base_units"])
		out, err = f.run(t, "stake", "locks")
		require.NoError(t, err)
		require.Empty(t, decodeObject(t, out)["locks"])
		require.NotContains(t, f.calls, "getDelegatorUnbondingLock")
	})
	t.Run("zero supply", func(t *testing.T) {
		f := newManagementFixture(t)
		f.reads["getGlobalTotalSupply"] = []any{new(big.Int)}
		out, err := f.run(t, "protocol", "get")
		require.NoError(t, err)
		require.Equal(t, "0", decodeObject(t, out)["participation_rate_percent"])
	})
	t.Run("invalid lock registration", func(t *testing.T) {
		f := newManagementFixture(t)
		f.reads["getDelegatorUnbondingLock"] = []any{new(big.Int), new(big.Int)}
		_, err := f.run(t, "orchestrator", "register", "--lock-id", "0", "--reward-cut", "1", "--fee-cut", "2")
		require.ErrorContains(t, err, "does not exist")
		require.Empty(t, f.simulations)
	})
	t.Run("invalid pool cycle", func(t *testing.T) {
		f := newManagementFixture(t)
		address := common.HexToAddress("0x1234")
		f.reads["getFirstTranscoderInPool"] = []any{address}
		f.reads["getNextTranscoderInPool"] = []any{address}
		_, err := f.run(t, "orchestrator", "list")
		require.ErrorContains(t, err, "invalid transcoder pool")
	})
}

func TestNoncanonicalInspectionFails(t *testing.T) {
	f := newManagementFixture(t)
	f.requireSnapshot, f.noncanonical = true, true
	out, err := f.run(t, "stake", "get")
	require.ErrorContains(t, err, "RPC error -32000")
	require.Empty(t, out)
	require.Equal(t, 1, f.snapshots)
}

func TestTextInspectionUnits(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, formatResult(&out, "text", map[string]any{
		"balance_wei": "1234567890123456789", "pending_stake_base_units": "1000000000000000001",
		"reward_cut_percent": "1.2345", "inflation": "10000000", "round_lock_amount": "125000",
		"fee_cap_wei": "203", "next_from_id": nil,
	}))
	require.Equal(t, "Balance: 1.234567890123456789 ETH\nFee cap wei: 203\nInflation: 1%\nNext from id: none\nPending stake: 1.000000000000000001 LPT\nReward cut percent: 1.2345%\nRound lock amount: 12.5%\n", out.String())
}
