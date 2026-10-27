// Ported with the implementation from go-livepeer at 0ff08af1bb4cb268f428d619cc968b5eaee0d093.
// Fixtures and repetitive cases are consolidated; fee-threshold expectations
// and upstream assertion typos are adapted. Behavioral assertions are retained.
package eth

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubTransactionSenderReader struct {
	err             map[string]error
	pending         bool
	tx              *types.Transaction
	receipt         *types.Receipt
	callsToTxByHash int
	lookupErrors    []error
	minedAfter      int
}

func (stm *stubTransactionSenderReader) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	return stm.err["SendTransaction"]
}

func (stm *stubTransactionSenderReader) TransactionByHash(ctx context.Context, txHash common.Hash) (tx *types.Transaction, isPending bool, err error) {
	stm.callsToTxByHash++
	pending := stm.pending
	if stm.minedAfter > 0 && stm.callsToTxByHash >= stm.minedAfter {
		pending = false
	}
	if stm.callsToTxByHash <= len(stm.lookupErrors) {
		return stm.tx, pending, stm.lookupErrors[stm.callsToTxByHash-1]
	}
	return stm.tx, pending, stm.err["TransactionByHash"]
}

func (stm *stubTransactionSenderReader) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	if stm.receipt == nil {
		return nil, ethereum.NotFound
	}
	return stm.receipt, stm.err["TransactionReceipt"]
}

func (stm *stubTransactionSenderReader) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	return []byte{}, stm.err["CodeAt"]
}

type stubTransactionSigner struct {
	err error
}

func (sig *stubTransactionSigner) SignTx(tx *types.Transaction) (*types.Transaction, error) {
	if sig.err != nil {
		return nil, sig.err
	}
	return tx, nil
}

func TestTransactionManager_SendTransaction(t *testing.T) {
	backend := &stubTransactionSenderReader{err: map[string]error{"SendTransaction": errors.New("send failed")}}
	tm := NewTransactionManager(backend, nil, nil, time.Second, 0)
	defer tm.Stop()
	tx := newStubLegacyTx(big.NewInt(10))
	require.ErrorIs(t, tm.SendTransaction(t.Context(), tx), backend.err["SendTransaction"])
	require.Empty(t, tm.queue, "a rejected send must not be tracked")
	backend.err = nil
	require.NoError(t, tm.SendTransaction(t.Context(), tx))
	require.Equal(t, []*types.Transaction{tx}, tm.queue)
	next := newStubLegacyTx(big.NewInt(20))
	tm.Track(next)
	require.Equal(t, []*types.Transaction{tx, next}, tm.queue, "Send and Track must enqueue in order")
}

func TestTransactionManager_Wait(t *testing.T) {
	assert := assert.New(t)

	eth := &stubTransactionSenderReader{
		err:     make(map[string]error),
		pending: true,
	}
	tm := NewTransactionManager(eth, nil, nil, 2*time.Second, 0)
	defer tm.Stop()

	// Test error
	// This calls bind.WaitMined() on the ethereum client, which will never actually return the actual underlying error and only log it using go-ethereum's custom logger
	// The expected error should thus be 'context deadline exceeded'
	// https://github.com/ethereum/go-ethereum/blob/aa637fd38a379db6da98df0d520fb1c5139a18ce/accounts/abi/bind/util.go#L41
	expErr := errors.New("context deadline exceeded")

	tx := types.NewTransaction(1, randomNonceAddress(), big.NewInt(100), 100000, big.NewInt(100), randomManagerBytes(68))

	receipt, err := tm.wait(tx)
	assert.Nil(receipt)
	assert.EqualError(err, expErr.Error())

	// No error, stub a receipt
	eth.receipt = types.NewReceipt(randomManagerHash().Bytes(), false, 100000)
	eth.err = nil

	receipt, err = tm.wait(tx)
	assert.Equal(receipt.Status, uint64(1))
	assert.Equal(receipt.CumulativeGasUsed, uint64(100000))
	assert.Nil(err)
}

func TestTransactionManager_Replace(t *testing.T) {
	assert := assert.New(t)

	eth := &stubTransactionSenderReader{
		err: make(map[string]error),
	}
	baseFee := big.NewInt(9)
	gasTipCap := big.NewInt(1)
	gasFeeCap := new(big.Int).Add(gasTipCap, new(big.Int).Mul(baseFee, big.NewInt(2)))
	gpm := &testFeeMonitor{
		maxGasPrice: big.NewInt(0),
	}
	tm := NewTransactionManager(eth, gpm, nil, 2*time.Second, 0)
	defer tm.Stop()

	stubTx := newStubDynamicFeeTx(gasFeeCap, gasTipCap)

	// Test eth.TransactionByHash error
	expErr := errors.New("TransactionByHash error")
	eth.err["TransactionByHash"] = expErr

	tx, err := tm.replace(stubTx)
	assert.Nil(tx)
	assert.EqualError(err, expErr.Error())
	eth.err["TransactionByHash"] = nil

	// Test no error - ErrReplacingMinedTx
	eth.pending = false
	tx, err = tm.replace(stubTx)
	assert.Nil(tx)
	assert.EqualError(err, ErrReplacingMinedTx.Error())

	// Test error is ethereum.NotFound - fail at next step
	eth.pending = true
	gpm.maxGasPrice = big.NewInt(1)
	eth.err["TransactionByHash"] = ethereum.NotFound
	tx, err = tm.replace(stubTx)
	assert.Nil(tx)
	assert.EqualError(
		err,
		fmt.Sprintf("replacement gas price exceeds max gas price suggested=%v max=%v", newReplacementTx(stubTx).GasFeeCap(), gpm.maxGasPrice),
	)
	eth.err["TransactionByHash"] = nil

	// Replacement gas price exceeds max gas price
	// Throw error
	tx, err = tm.replace(stubTx)
	assert.Nil(tx)
	assert.EqualError(
		err,
		fmt.Sprintf("replacement gas price exceeds max gas price suggested=%v max=%v", newReplacementTx(stubTx).GasFeeCap(), gpm.maxGasPrice),
	)

	// Error signing replacement tx
	expErr = errors.New("SignTx error")
	sig := &stubTransactionSigner{
		err: nil,
	}
	tm.sig = sig
	sig.err = expErr
	gpm.maxGasPrice = big.NewInt(99999)
	tx, err = tm.replace(stubTx)
	assert.Nil(tx)
	assert.EqualError(err, expErr.Error())

	// Test when max gas price is nil - should still return signing replacement tx error
	gpm.maxGasPrice = nil
	tx, err = tm.replace(stubTx)
	assert.Nil(tx)
	assert.EqualError(err, expErr.Error())
	sig.err = nil

	// Error sending replacement tx
	expErr = errors.New("SendTx error")
	eth.err["SendTransaction"] = expErr
	tx, err = tm.replace(stubTx)
	assert.NotNil(tx)
	assert.EqualError(err, expErr.Error())
	eth.err["SendTransaction"] = nil

	// Success
	tx, err = tm.replace(stubTx)
	assert.Nil(err)
	expTx := newReplacementTx(stubTx)
	assert.Equal(tx.Hash(), expTx.Hash())
}

func TestTransactionManager_CheckTxLoop(t *testing.T) {
	lookupErr := errors.New("TransactionByHash error")
	for _, tc := range []struct {
		name                  string
		mined                 bool
		replacements, lookups int
		minedAfter            int
		lookupErr, wantErr    error
	}{
		{name: "included", mined: true},
		{name: "timeout without replacements", lookups: 1, wantErr: context.DeadlineExceeded},
		{name: "replacement error", replacements: 1, lookups: 2, lookupErr: lookupErr, wantErr: lookupErr},
		{name: "replacement error stops attempts", replacements: 3, lookups: 2, lookupErr: lookupErr, wantErr: lookupErr},
		{name: "included before replacement", replacements: 1, lookups: 2, minedAfter: 2, wantErr: ErrReplacingMinedTx},
		{name: "replacement limit", replacements: 3, lookups: 10, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &stubTransactionSenderReader{pending: true, err: map[string]error{"TransactionByHash": tc.lookupErr}}
			// The first live check succeeds; the replacement itself can fail.
			backend.lookupErrors = []error{nil}
			backend.minedAfter = tc.minedAfter
			if tc.mined {
				backend.receipt = types.NewReceipt(randomManagerHash().Bytes(), false, 100000)
			}
			tm := NewTransactionManager(backend, &testFeeMonitor{maxGasPrice: big.NewInt(99999999)}, &stubTransactionSigner{}, time.Millisecond, tc.replacements)
			sink := make(chan *transactionReceipt, 1)
			tm.Completed = func(_ context.Context, result *transactionReceipt) { sink <- result }
			done := make(chan struct{})
			go func() { defer close(done); tm.Start() }()
			defer func() {
				tm.Stop()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("transaction manager did not stop")
				}
			}()
			tx := newStubLegacyTx(big.NewInt(10))
			require.NoError(t, tm.SendTransaction(t.Context(), tx))
			select {
			case event := <-sink:
				require.NotNil(t, event)
				require.Equal(t, tx.Hash(), event.originTxHash)
				require.ErrorIs(t, event.err, tc.wantErr)
				require.Equal(t, tc.lookups, backend.callsToTxByHash)
				if errors.Is(event.err, ErrReplacingMinedTx) {
					require.Equal(t, tx.Hash(), event.TxHash)
				}
				if tc.mined {
					require.Equal(t, backend.receipt, &event.Receipt)
				}
			case <-time.After(time.Second):
				t.Fatal("transaction result was not delivered")
			}
		})
	}
}

func TestApplyPriceBump(t *testing.T) {
	for _, tc := range []struct{ value, bump, want int64 }{
		{500, 0, 500}, {500, 11, 555}, {500, 17, 585}, {500, 101, 1005},
		{50, 11, 56}, {501, 11, 557}, {9, 11, 10},
	} {
		assert.Equal(t, big.NewInt(tc.want), applyPriceBump(big.NewInt(tc.value), uint64(tc.bump)), "value=%d bump=%d", tc.value, tc.bump)
	}
}

func TestNewReplacementTx(t *testing.T) {
	for name, tx1 := range map[string]*types.Transaction{
		"legacy":  newStubLegacyTx(big.NewInt(100)),
		"dynamic": newStubDynamicFeeTx(big.NewInt(1000), big.NewInt(100)),
	} {
		t.Run(name, func(t *testing.T) {
			tx2 := newReplacementTx(tx1)
			assert.NotEqual(t, tx1.Hash(), tx2.Hash())
			assert.Equal(t, applyPriceBump(tx1.GasTipCap(), priceBump), tx2.GasTipCap())
			assert.Equal(t, applyPriceBump(tx1.GasFeeCap(), priceBump), tx2.GasFeeCap())
			assert.Equal(t, tx1.Nonce(), tx2.Nonce())
			assert.Equal(t, tx1.Gas(), tx2.Gas())
			assert.Equal(t, tx1.Value(), tx2.Value())
			assert.Equal(t, tx1.To(), tx2.To())
			assert.Equal(t, tx1.Data(), tx2.Data())
		})
	}
}

func newStubLegacyTx(gasPrice *big.Int) *types.Transaction {
	addr := randomNonceAddress()
	return types.NewTx(&types.LegacyTx{
		Nonce:    1,
		GasPrice: gasPrice,
		Gas:      1000000,
		Value:    big.NewInt(100),
		Data:     randomManagerBytes(68),
		To:       &addr,
	})
}

func newStubDynamicFeeTx(gasFeeCap, gasTipCap *big.Int) *types.Transaction {
	addr := randomNonceAddress()
	return types.NewTx(&types.DynamicFeeTx{
		Nonce:     1,
		GasFeeCap: gasFeeCap,
		GasTipCap: gasTipCap,
		Gas:       1000000,
		Value:     big.NewInt(100),
		Data:      randomManagerBytes(68),
		To:        &addr,
	})
}

// Upstream gas-monitor tests only need a mutable ceiling; production uses the
// existing Contracts fee ceiling instead of importing its gas monitor service.
type testFeeMonitor struct{ maxGasPrice *big.Int }

func (m *testFeeMonitor) MaxGasPrice() *big.Int { return m.maxGasPrice }
func randomManagerBytes(n int) []byte           { b := make([]byte, n); _, _ = rand.Read(b); return b }
func randomManagerHash() common.Hash            { return common.BytesToHash(randomManagerBytes(32)) }

func TestGethReplacementThresholdStrictlyIncreasesZero(t *testing.T) {
	assert.Equal(t, big.NewInt(1), applyPriceBump(new(big.Int), priceBump))
	assert.Equal(t, big.NewInt(110), applyPriceBump(big.NewInt(100), priceBump))
	original := types.NewTx(&types.DynamicFeeTx{ChainID: big.NewInt(42161), Nonce: 7, GasFeeCap: big.NewInt(100), GasTipCap: new(big.Int), Gas: 21000, To: new(common.Address), AccessList: types.AccessList{{Address: common.HexToAddress("0x1234")}}})
	next := newReplacementTx(original)
	assert.Equal(t, original.ChainId(), next.ChainId())
	assert.Equal(t, original.AccessList(), next.AccessList())
	assert.Equal(t, big.NewInt(1), next.GasTipCap())
}

func TestLegacyReplacementRetainsSignerChain(t *testing.T) {
	path, password := test.WriteFixedKeystore(t)
	key, err := OpenKeystoreFile(path, password)
	require.NoError(t, err)
	original, err := key.SignTxForChain(types.NewTx(&types.LegacyTx{Nonce: 7, GasPrice: big.NewInt(100), Gas: 21000, To: new(common.Address)}), big.NewInt(42161))
	require.NoError(t, err)
	backend := &stubTransactionSenderReader{pending: true, err: map[string]error{}}
	tm := NewTransactionManager(backend, &testFeeMonitor{}, key, time.Second, 1)
	defer tm.Stop()
	replacement, err := tm.replace(original)
	require.NoError(t, err)
	require.Equal(t, original.ChainId(), replacement.ChainId())
	from, err := types.Sender(types.LatestSignerForChainID(original.ChainId()), replacement)
	require.NoError(t, err)
	require.Equal(t, key.Address(), from)
}

func TestStoppedManagerDoesNotDrainQueue(t *testing.T) {
	tm := NewTransactionManager(&stubTransactionSenderReader{}, nil, nil, time.Hour, 0)
	restores := 0
	tm.Restore = func(ctx context.Context, tx *types.Transaction) (TransactionRecovery, error) {
		restores++
		return TransactionRecovery{}, ctx.Err()
	}
	for i := 0; i < 3; i++ {
		tm.Track(types.NewTx(&types.LegacyTx{Nonce: uint64(i)}))
	}
	tm.Stop()
	tm.Start()
	require.Zero(t, restores, "stopped manager must not run restore callbacks for queued work")
	require.Len(t, tm.queue, 3, "stopped manager must leave queued work intact")
}
