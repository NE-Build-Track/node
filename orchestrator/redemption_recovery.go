// Recovery adapters for the go-livepeer port at
// 0ff08af1bb4cb268f428d619cc968b5eaee0d093. These bridge node's durable store
// and finalized settlement to the upstream nonce/send/wait/replace lifecycle.
package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth"
)

type RedemptionOptions struct {
	Timeout         time.Duration
	MaxReplacements int
}

var defaultRedemptionOptions = RedemptionOptions{Timeout: 5 * time.Minute, MaxReplacements: 1}

type redemptionRecovery struct {
	store         *RedeemerDB
	chain         eth.PaymentChain
	key           *eth.Key
	nonces        *eth.NonceManager
	manager       *eth.TransactionManager
	mu            sync.Mutex
	tracking      map[common.Hash]bool
	pendingErrors []error
	done          chan struct{}
	closeOnce     sync.Once
	stopParent    func() bool
}

// StartRedemptionRecovery binds long inclusion waits to the service lifetime,
// independently of the bounded heartbeat contexts used to schedule tickets.
func StartRedemptionRecovery(ctx context.Context, store *RedeemerDB, chain eth.PaymentChain, key *eth.Key, options RedemptionOptions) error {
	if store == nil || key == nil || chain.Contracts == nil || chain.Contracts.RPC == nil {
		return errors.New("redemption is not configured")
	}
	if options.Timeout <= 0 || options.MaxReplacements < 0 {
		return errors.New("invalid redemption recovery options")
	}
	_, err := store.recoveryFor(ctx, chain, key, options)
	return err
}
func (s *RedeemerDB) recoveryFor(ctx context.Context, chain eth.PaymentChain, key *eth.Key, options RedemptionOptions) (*redemptionRecovery, error) {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.recovery != nil {
		return s.recovery, nil
	}
	r := &redemptionRecovery{store: s, chain: chain, key: key, tracking: make(map[common.Hash]bool), done: make(chan struct{})}
	r.nonces = eth.NewNonceManager(r)
	floor, err := s.SavedNonceFloor(ctx, key.Address())
	if err != nil {
		return nil, err
	}
	r.nonces.Restore(key.Address(), floor)
	r.manager = eth.NewTransactionManager(r, r, key, options.Timeout, options.MaxReplacements)
	r.manager.Restore = r.restore
	r.manager.BeforeReplacement = r.prepareReplacement
	r.manager.Completed = r.completed
	r.stopParent = context.AfterFunc(ctx, r.manager.Stop)
	go func() { defer close(r.done); r.manager.Start() }()
	s.recovery = r
	return r, nil
}
func (r *redemptionRecovery) close() {
	r.closeOnce.Do(func() { r.stopParent(); r.manager.Stop(); <-r.done })
}
func (r *redemptionRecovery) MaxGasPrice() *big.Int { return r.chain.Contracts.MaxFeePerGas }

// Next holds the upstream account lock while reading both sources. Receipt
// reconciliation and a separate CLI process can acknowledge sends after this
// worker starts, so the startup cursor alone cannot cover stale pending reads.
func (r *redemptionRecovery) PendingNonceAt(ctx context.Context, address common.Address) (uint64, error) {
	pending, err := r.chain.Contracts.RPC.PendingNonceAt(ctx, address)
	if err != nil {
		return 0, err
	}
	floor, err := r.store.SavedNonceFloor(ctx, address)
	if err != nil {
		return 0, err
	}
	return r.repairNonceGap(ctx, address, pending, floor)
}

func (r *redemptionRecovery) CodeAt(ctx context.Context, a common.Address, n *big.Int) ([]byte, error) {
	return r.chain.Contracts.RPC.CodeAt(ctx, a, n)
}
func (r *redemptionRecovery) TransactionByHash(ctx context.Context, h common.Hash) (*types.Transaction, bool, error) {
	return r.chain.Contracts.RPC.TransactionByHash(ctx, h)
}
func (r *redemptionRecovery) TransactionReceipt(ctx context.Context, h common.Hash) (*types.Receipt, error) {
	return r.chain.InclusionReceipt(ctx, h)
}

func (r *redemptionRecovery) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	var sig []byte
	var root string
	var previous sql.NullString
	if err := r.store.db.QueryRowContext(ctx, "SELECT sig,root_hash,replaces_hash FROM redemption_transactions WHERE hash=?", tx.Hash().Hex()).Scan(&sig, &root, &previous); err != nil {
		return err
	}
	r.mu.Lock()
	r.tracking[common.HexToHash(root)] = true
	r.mu.Unlock()
	identity, err := signedIdentity(tx)
	if err == nil {
		err = broadcastRedemption(ctx, r.store, r.chain.Contracts, sig, identity)
	}
	if err != nil && !previous.Valid {
		r.mu.Lock()
		delete(r.tracking, common.HexToHash(root))
		r.mu.Unlock()
	}
	return err
}
func (r *redemptionRecovery) prepareReplacement(ctx context.Context, previous, next *types.Transaction) error {
	identity, err := signedIdentity(next)
	if err != nil {
		return err
	}
	if identity.From != r.key.Address() {
		return errors.New("replacement transaction account mismatch")
	}
	var sig []byte
	if err := r.store.db.QueryRowContext(ctx, "SELECT sig FROM redemption_transactions WHERE hash=?", previous.Hash().Hex()).Scan(&sig); err != nil {
		return err
	}
	return r.store.recordTransaction(ctx, sig, identity, previous.Hash().Hex())
}
func (r *redemptionRecovery) restore(ctx context.Context, tx *types.Transaction) (eth.TransactionRecovery, error) {
	var root string
	var phase, identityPhase string
	err := r.store.db.QueryRowContext(ctx, `SELECT t.root_hash,a.phase,t.phase FROM redemption_transactions t JOIN redemption_attempts a ON a.sig=t.sig WHERE t.hash=?`, tx.Hash().Hex()).Scan(&root, &phase, &identityPhase)
	if err != nil {
		return eth.TransactionRecovery{}, err
	}
	if terminalRedemption(phase) || identityPhase == "superseded" {
		return eth.TransactionRecovery{OriginHash: common.HexToHash(root)}, context.Canceled
	}
	rows, err := r.store.db.QueryContext(ctx, `SELECT t.hash,t.raw_transaction,COALESCE(t.broadcast_at,t.prepared_at),w.tx_hash=t.hash FROM redemption_transactions t JOIN winning_tickets w ON w.sig=t.sig WHERE t.root_hash=? ORDER BY t.rowid`, root)
	if err != nil {
		return eth.TransactionRecovery{}, err
	}
	defer rows.Close()
	state := eth.TransactionRecovery{OriginHash: common.HexToHash(root), Replacements: -1}
	for rows.Next() {
		var hash, at string
		var raw []byte
		var current bool
		if err := rows.Scan(&hash, &raw, &at, &current); err != nil {
			return state, err
		}
		latest := new(types.Transaction)
		if err := latest.UnmarshalBinary(raw); err != nil {
			return state, errors.New("invalid stored redemption transaction")
		}
		state.Hashes = append(state.Hashes, common.HexToHash(hash))
		state.Replacements++
		if current {
			state.Transaction = latest
			state.SentAt, err = time.Parse(time.RFC3339Nano, at)
			if err != nil {
				return state, errors.New("invalid stored redemption timestamp")
			}
		}
	}
	if err := rows.Err(); err != nil {
		return state, err
	}
	if state.Transaction == nil {
		return state, context.Canceled
	}
	return state, nil
}
func (r *redemptionRecovery) completed(ctx context.Context, result *eth.TransactionReceipt) {
	defer func() { r.mu.Lock(); delete(r.tracking, result.OriginHash()); r.mu.Unlock() }()
	if err := r.recordResult(ctx, result); err != nil && !errors.Is(err, context.Canceled) {
		r.mu.Lock()
		r.pendingErrors = append(r.pendingErrors, err)
		r.mu.Unlock()
	}
}
func (r *redemptionRecovery) recordResult(ctx context.Context, result *eth.TransactionReceipt) error {
	root := result.OriginHash().Hex()
	if errors.Is(result.Error(), context.Canceled) || errors.Is(result.Error(), eth.ErrReceiptUnavailable) {
		return nil
	}
	phase := "mined"
	var message any
	if result.Error() != nil && !errors.Is(result.Error(), eth.ErrReplacingMinedTx) {
		phase = "retryable"
		message = result.Error().Error()
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if phase == "mined" && result.TxHash != (common.Hash{}) {
		_, err = tx.ExecContext(ctx, `UPDATE redemption_transactions SET phase='mined',error=NULL,acknowledged=1 WHERE hash=? AND phase NOT IN ('confirmed','reverted','superseded')`, result.TxHash.Hex())
	} else if phase == "retryable" {
		_, err = tx.ExecContext(ctx, `UPDATE redemption_transactions SET phase=?,error=? WHERE root_hash=? AND phase IN ('prepared','broadcast','sending','submitted','mined')`, phase, message, root)
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE redemption_attempts SET phase=?,error=? WHERE phase IN ('prepared','broadcast','submitted','mined') AND sig IN (SELECT sig FROM redemption_transactions WHERE root_hash=?) AND EXISTS(SELECT 1 FROM winning_tickets w JOIN redemption_transactions t ON t.hash=w.tx_hash WHERE w.sig=redemption_attempts.sig AND t.root_hash=?)`, phase, message, root, root)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func terminalRedemption(phase string) bool {
	return phase == "confirmed" || phase == "reverted" || phase == "used" || phase == "invalid" || phase == "expired"
}

// resume first restores each active upstream queue job. Old 'sending' rows are
// uncertain sends, not permanent blockers. Failed groups remain receipt-visible.
func (r *redemptionRecovery) resume(ctx context.Context) []error {
	failures := r.reconcileUsed(ctx)
	// Repair old reservations even when no eligible ticket triggers Next. Never
	// reset the upstream cursor on an ambiguous pending-nonce observation.
	floor, err := r.store.SavedNonceFloor(ctx, r.key.Address())
	if err == nil && floor > 0 {
		r.nonces.Lock(r.key.Address())
		_, err = r.PendingNonceAt(ctx, r.key.Address())
		r.nonces.Unlock(r.key.Address())
	}
	if err != nil && !errors.Is(err, errRecoveringNonce) {
		failures = append(failures, err)
	}
	r.mu.Lock()
	failures = append(failures, r.pendingErrors...)
	r.pendingErrors = nil
	r.mu.Unlock()
	rows, err := r.store.db.QueryContext(ctx, `SELECT t.root_hash,t.raw_transaction,t.phase FROM winning_tickets w JOIN redemption_attempts a ON a.sig=w.sig JOIN redemption_transactions t ON t.hash=w.tx_hash WHERE a.phase IN ('prepared','broadcast','submitted','mined') ORDER BY t.rowid`)
	if err != nil {
		return []error{err}
	}
	type job struct {
		root  common.Hash
		raw   []byte
		phase string
	}
	var jobs []job
	for rows.Next() {
		var root string
		var j job
		if err := rows.Scan(&root, &j.raw, &j.phase); err != nil {
			rows.Close()
			return []error{err}
		}
		j.root = common.HexToHash(root)
		jobs = append(jobs, j)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return []error{err}
	}
	for _, j := range jobs {
		r.mu.Lock()
		active := r.tracking[j.root]
		if !active {
			r.tracking[j.root] = true
		}
		r.mu.Unlock()
		if active {
			continue
		}
		tx := new(types.Transaction)
		err := tx.UnmarshalBinary(j.raw)
		if err == nil {
			if j.phase == "prepared" || j.phase == "broadcast" || j.phase == "sending" {
				// No live sender exists for this group; recover a crash during a send.
				if j.phase == "sending" {
					_, err = r.store.db.ExecContext(ctx, "UPDATE redemption_transactions SET phase='broadcast' WHERE hash=? AND phase='sending'", tx.Hash().Hex())
				}
				if err == nil {
					err = r.manager.SendTransaction(ctx, tx)
				}
				if err == nil {
					identity, e := signedIdentity(tx)
					if e == nil && identity.Nonce < ^uint64(0) {
						r.nonces.Restore(identity.From, identity.Nonce+1)
					}
				}
			} else {
				r.manager.Track(tx)
			}
		}
		if err != nil {
			failures = append(failures, err)
			r.mu.Lock()
			delete(r.tracking, j.root)
			r.mu.Unlock()
		}
	}
	return failures
}

// Keep this adapter's interface narrow despite geth's broader embedded readers.
var _ ethereum.TransactionSender = (*redemptionRecovery)(nil)

// A latest-state used ticket pauses the upstream queue. Settlement remains pinned
// to finalized state; a reorg that removes that use makes the ticket retryable.
func (r *redemptionRecovery) reconcileUsed(ctx context.Context) []error {
	rows, err := r.store.db.QueryContext(ctx, `SELECT sig,phase FROM redemption_attempts a WHERE phase IN ('awaiting_finality','mined') OR (phase='reverted' AND EXISTS(SELECT 1 FROM redemption_transactions t WHERE t.sig=a.sig AND t.phase NOT IN ('confirmed','reverted','superseded')))`)
	if err != nil {
		return []error{err}
	}
	type waiting struct {
		sig   []byte
		phase string
	}
	var signatures []waiting
	for rows.Next() {
		var item waiting
		if err := rows.Scan(&item.sig, &item.phase); err != nil {
			rows.Close()
			return []error{err}
		}
		signatures = append(signatures, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return []error{err}
	}
	var failures []error
	for _, item := range signatures {
		sig := item.sig
		ticket, err := r.store.winningTicket(ctx, sig)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		used, err := r.chain.IsUsedTicket(ctx, ticket.Hash(), "latest")
		if err != nil {
			failures = append(failures, err)
			continue
		}
		phase := "retryable"
		if !used && item.phase != "awaiting_finality" {
			continue
		}
		if used {
			finalized, err := r.chain.IsUsedTicket(ctx, ticket.Hash(), "finalized")
			if err != nil {
				failures = append(failures, err)
				continue
			}
			if !finalized {
				continue
			}
			phase = "used"
		}
		if err := r.store.setTicketPhase(ctx, sig, phase); err != nil {
			failures = append(failures, err)
		}
	}
	return failures
}
