// Ported from livepeer/go-livepeer at 0ff08af1bb4cb268f428d619cc968b5eaee0d093.
// Original implementation by Yondon Fu and Rafal Leszko; MIT notice in LICENSE.
package eth

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Geth defaults to txpool.pricebump=10. Its RPC client only submits signed
// transactions; it does not create replacements. Keep upstream replacement
// control flow, using geth's default threshold instead of its extra 1% margin.
const priceBump uint64 = 10

var ErrReplacingMinedTx = errors.New("cannot replace a mined transaction")

// Transaction lookup proves inclusion of this hash, not a finalized receipt.
// Keep the identity on the completion result so other attempts stay unresolved.
type includedTransaction struct{ hash ethcommon.Hash }

func (e includedTransaction) Error() string { return ErrReplacingMinedTx.Error() }
func (e includedTransaction) Unwrap() error { return ErrReplacingMinedTx }

// A failed live receipt check cannot establish that an inclusion timed out.
// Durable callers retain the monitoring cursor and try the check again.
var ErrReceiptUnavailable = errors.New("transaction inclusion could not be checked")

type feeCeiling interface{ MaxGasPrice() *big.Int }

// TransactionRecovery is the persisted upstream inclusion/replacement cursor.
type TransactionRecovery struct {
	OriginHash   ethcommon.Hash
	Transaction  *types.Transaction
	Hashes       []ethcommon.Hash
	SentAt       time.Time
	Replacements int
}

type transactionSenderReader interface {
	ethereum.TransactionSender
	TransactionByHash(context.Context, ethcommon.Hash) (*types.Transaction, bool, error)
	TransactionReceipt(context.Context, ethcommon.Hash) (*types.Receipt, error)
	// Required for bind.DeployBackend argument in bind.WaitMined
	CodeAt(context.Context, ethcommon.Address, *big.Int) ([]byte, error)
}

type transactionSigner interface {
	SignTx(tx *types.Transaction) (*types.Transaction, error)
}

type TransactionManager struct {
	txTimeout       time.Duration
	maxReplacements int

	queue []*types.Transaction

	eth transactionSenderReader
	gpm feeCeiling
	sig transactionSigner

	cond *sync.Cond

	quit     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	// Persistence adapters run before any replacement can reach the network.
	Restore           func(context.Context, *types.Transaction) (TransactionRecovery, error)
	BeforeReplacement func(context.Context, *types.Transaction, *types.Transaction) error
	Completed         func(context.Context, *transactionReceipt)
	current           TransactionRecovery
}

type transactionReceipt struct {
	originTxHash ethcommon.Hash
	types.Receipt
	err error
}

func NewTransactionManager(eth transactionSenderReader, gpm feeCeiling, signer transactionSigner, txTimeout time.Duration, maxReplacements int) *TransactionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &TransactionManager{
		ctx: ctx, cancel: cancel,
		cond:            sync.NewCond(&sync.Mutex{}),
		txTimeout:       txTimeout,
		maxReplacements: maxReplacements,
		eth:             eth,
		gpm:             gpm,
		sig:             signer,
		quit:            make(chan struct{}),
	}
}

func (tm *TransactionManager) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	sendErr := tm.eth.SendTransaction(ctx, tx)

	if sendErr != nil {
		return sendErr
	}

	tm.Track(tx)
	return nil
}

func (tm *TransactionManager) Start() {
	tm.checkTxLoop()
}

func (tm *TransactionManager) Stop() {
	tm.stopOnce.Do(func() {
		tm.cancel()
		tm.cond.L.Lock()
		close(tm.quit)
		tm.cond.Broadcast()
		tm.cond.L.Unlock()
	})
}

// Track resumes an identity already durably submitted, without broadcasting.
func (tm *TransactionManager) Track(tx *types.Transaction) {
	tm.cond.L.Lock()
	tm.queue = append(tm.queue, tx)
	tm.cond.L.Unlock()
	tm.cond.Signal()
}

func (tm *TransactionManager) Context() context.Context {
	return tm.ctx
}

func (tm *TransactionManager) wait(tx *types.Transaction) (*types.Receipt, error) {
	ctx, cancel := context.WithTimeout(tm.Context(), tm.txTimeout)
	defer cancel()

	// Resume the original deadline after restart, rather than buying a new timeout.
	if !tm.current.SentAt.IsZero() {
		cancel()
		ctx, cancel = context.WithDeadline(tm.Context(), tm.current.SentAt.Add(tm.txTimeout))
		defer cancel()
	}
	reader := receiptAttempts{transactionSenderReader: tm.eth, hashes: tm.current.Hashes}
	receipt, err := bind.WaitMined(ctx, reader, tx)
	if errors.Is(err, context.DeadlineExceeded) && tm.Context().Err() == nil {
		// Inclusion deadlines survive restart; canonical inclusion does too. Use
		// a live context before replacing or reopening a ticket's retry queue.
		found, lookupErr := reader.TransactionReceipt(tm.Context(), tx.Hash())
		if lookupErr == nil {
			return found, nil
		}
		if !errors.Is(lookupErr, ethereum.NotFound) {
			return nil, fmt.Errorf("%w: %w", ErrReceiptUnavailable, lookupErr)
		}
		// Receipt endpoints can lag transaction lookup. Preserve upstream's
		// mined guard even when no replacements remain, for every saved hash.
		hashes := tm.current.Hashes
		if len(hashes) == 0 {
			hashes = []ethcommon.Hash{tx.Hash()}
		}
		var inclusionErr error
		for _, hash := range hashes {
			_, pending, lookupErr := tm.eth.TransactionByHash(tm.Context(), hash)
			if lookupErr == nil && !pending {
				return nil, includedTransaction{hash}
			}
			if lookupErr != nil && !errors.Is(lookupErr, ethereum.NotFound) {
				inclusionErr = errors.Join(inclusionErr, lookupErr)
			}
		}
		if inclusionErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrReceiptUnavailable, inclusionErr)
		}
	}
	return receipt, err
}

func (tm *TransactionManager) replace(tx *types.Transaction) (*types.Transaction, error) {
	_, pending, err := tm.eth.TransactionByHash(tm.Context(), tx.Hash())
	// Only return here if the error is not related to the tx not being found
	// Presumably the provided tx was already broadcasted at some point, so even if for some reason the
	// node being used cannot find it, the originally broadcasted tx is still valid and might be sitting somewhere
	if err != nil && !errors.Is(err, ethereum.NotFound) {
		return nil, err
	}
	// If tx was found
	// If `pending` is false, the tx was mined and included in a block
	if err == nil && !pending {
		return nil, includedTransaction{tx.Hash()}
	}

	// A restarted node may have rotated keys. Stored identities retain their
	// original sender; never sign a replacement using a different account.
	if signer, ok := tm.sig.(interface{ Address() ethcommon.Address }); ok {
		from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
		if err != nil || from != signer.Address() {
			return nil, errors.New("replacement transaction account mismatch")
		}
	}
	newRawTx := newReplacementTx(tx)

	// Bump gas price exceeds max gas price, return early
	max := tm.gpm.MaxGasPrice()
	// Node's ceiling bounds the actual fee cap, including EIP-1559 worst case.
	newGasPrice := newRawTx.GasFeeCap()
	if max != nil && newGasPrice.Cmp(max) > 0 {
		return nil, fmt.Errorf("replacement gas price exceeds max gas price suggested=%v max=%v", newGasPrice, max)
	}

	var newSignedTx *types.Transaction
	// Upstream accountManager already knows its chain. Node's raw Key needs
	// that binding explicitly for unsigned legacy replacements (chain ID zero).
	if signer, ok := tm.sig.(interface {
		SignTxForChain(*types.Transaction, *big.Int) (*types.Transaction, error)
	}); ok {
		newSignedTx, err = signer.SignTxForChain(newRawTx, tx.ChainId())
	} else {
		newSignedTx, err = tm.sig.SignTx(newRawTx)
	}
	if err != nil {
		return nil, err
	}

	if tm.BeforeReplacement != nil {
		if err := tm.BeforeReplacement(tm.Context(), tx, newSignedTx); err != nil {
			return nil, err
		}
	}
	sendErr := tm.eth.SendTransaction(tm.Context(), newSignedTx)
	if sendErr == nil {
		tm.current.SentAt = time.Now()
		tm.current.Hashes = append(tm.current.Hashes, newSignedTx.Hash())
	}

	return newSignedTx, sendErr
}

func (tm *TransactionManager) checkTxLoop() {
	for {
		tm.cond.L.Lock()
		for len(tm.queue) == 0 {
			select {
			case <-tm.quit:
				tm.cond.L.Unlock()
				return
			default:
			}
			tm.cond.Wait()
		}
		// Stop must also leave already queued jobs for durable restart recovery.
		select {
		case <-tm.quit:
			tm.cond.L.Unlock()
			return
		default:
		}

		tx := tm.queue[0]
		tm.queue = tm.queue[1:]
		tm.cond.L.Unlock()

		originHash := tx.Hash()
		tm.current = TransactionRecovery{}
		if tm.Restore != nil {
			restored, err := tm.Restore(tm.Context(), tx)
			if restored.OriginHash != (ethcommon.Hash{}) {
				originHash = restored.OriginHash
			}
			if err != nil {
				tm.deliver(&transactionReceipt{originTxHash: originHash, err: err})
				continue
			}
			tm.current = restored
			if restored.Transaction != nil {
				tx = restored.Transaction
			}
		}

		var txReceipt types.Receipt

		receipt, err := tm.wait(tx)

		// context.DeadlineExceeded indicates that we hit the txTimeout
		// If we hit the txTimeout, replace the tx up to maxReplacements times
		for i := tm.current.Replacements; err == context.DeadlineExceeded && i < tm.maxReplacements; i++ {
			tx, err = tm.replace(tx)
			// Do not attempt additional replacements if there was an error submitting this
			// replacement tx
			if err != nil {
				break
			}
			receipt, err = tm.wait(tx)
		}

		if receipt == nil {
			txReceipt = types.Receipt{}
		} else {
			txReceipt = *(receipt)
		}
		var included includedTransaction
		if errors.As(err, &included) {
			txReceipt.TxHash = included.hash
		}

		tm.deliver(&transactionReceipt{
			originTxHash: originHash,
			Receipt:      txReceipt,
			err:          err,
		})

	}
}

func (tm *TransactionManager) deliver(result *transactionReceipt) {
	if tm.Completed != nil {
		tm.Completed(tm.Context(), result)
	}
}

// receiptAttempts lets upstream WaitMined observe any persisted predecessor.
// RPC validation still checks each returned receipt against its requested hash.
type receiptAttempts struct {
	transactionSenderReader
	hashes []ethcommon.Hash
}

func (r receiptAttempts) TransactionReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	if len(r.hashes) == 0 {
		return r.transactionSenderReader.TransactionReceipt(ctx, hash)
	}
	var lookupErr error
	for _, h := range r.hashes {
		receipt, err := r.transactionSenderReader.TransactionReceipt(ctx, h)
		if err == nil {
			return receipt, nil
		}
		if !errors.Is(err, ethereum.NotFound) {
			lookupErr = errors.Join(lookupErr, err)
		}
	}
	if lookupErr != nil {
		return nil, lookupErr
	}
	return nil, ethereum.NotFound
}

func applyPriceBump(val *big.Int, priceBump uint64) *big.Int {
	a := big.NewInt(100 + int64(priceBump))
	b := new(big.Int).Mul(a, val)
	// div round up
	b.Div(new(big.Int).Add(b, big.NewInt(99)), big.NewInt(100))
	// Geth also requires a strict increase, including for zero-valued tips.
	if priceBump > 0 && b.Cmp(val) <= 0 {
		b.Add(val, big.NewInt(1))
	}
	return b
}

func newReplacementTx(tx *types.Transaction) *types.Transaction {
	var baseTx types.TxData
	if tx.Type() == types.LegacyTxType {
		// legacy tx, not London ready
		baseTx = &types.LegacyTx{
			Nonce:    tx.Nonce(),
			GasPrice: applyPriceBump(tx.GasPrice(), priceBump),
			Gas:      tx.Gas(),
			To:       tx.To(),
			Value:    tx.Value(),
			Data:     tx.Data(),
		}
	} else {
		baseTx = &types.DynamicFeeTx{
			ChainID: tx.ChainId(), AccessList: tx.AccessList(),
			Nonce: tx.Nonce(),
			// geth requires the price bump to be applied to both the gas tip cap and gas fee cap
			GasFeeCap: applyPriceBump(tx.GasFeeCap(), priceBump),
			GasTipCap: applyPriceBump(tx.GasTipCap(), priceBump),
			Gas:       tx.Gas(),
			Value:     tx.Value(),
			Data:      tx.Data(),
			To:        tx.To(),
		}
	}

	return types.NewTx(baseTx)
}

func (r *transactionReceipt) OriginHash() ethcommon.Hash { return r.originTxHash }
func (r *transactionReceipt) Error() error               { return r.err }

type TransactionReceipt = transactionReceipt
