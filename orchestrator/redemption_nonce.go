package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var errRecoveringNonce = errors.New("recovering an outstanding redemption nonce")

type finalizedNonceReader interface {
	FinalizedNonce(context.Context, common.Address) (uint64, error)
	InclusionReceipt(context.Context, common.Hash) (*types.Receipt, error)
}

// repairNonceGap runs under the account lock. Acknowledged reservations remain
// monotonic: repair missing slots with their saved bytes instead of lowering the
// cursor on a possibly stale RPC observation. Ticket settlement at another nonce
// does not prove that a superseded identity consumed this slot.
func (r *redemptionRecovery) repairNonceGap(ctx context.Context, address common.Address, pending, floor uint64) (uint64, error) {
	// Finalized state may be ahead of every local identity, even when all saved
	// slots already have receipts. Carry that observation into fresh allocation.
	finalized, err := r.chain.FinalizedNonce(ctx, address)
	if err != nil {
		return floor, err
	}
	allocated := max(pending, floor, finalized)
	pending = max(pending, finalized)
	for ; pending < floor; pending++ {
		rows, err := r.store.db.QueryContext(ctx, `SELECT hash,root_hash,sig,raw_transaction,phase FROM redemption_transactions WHERE redeemer_address=? AND nonce=? ORDER BY rowid DESC`, address.Hex(), strconv.FormatUint(pending, 10))
		if err != nil {
			return allocated, err
		}
		type saved struct {
			hash, root, phase string
			sig, raw          []byte
		}
		var identities []saved
		for rows.Next() {
			var item saved
			if err = rows.Scan(&item.hash, &item.root, &item.sig, &item.raw, &item.phase); err != nil {
				break
			}
			identities = append(identities, item)
		}
		err = errors.Join(err, rows.Err(), rows.Close())
		if err != nil {
			return allocated, err
		}
		present := false
		for _, item := range identities {
			if item.phase == "confirmed" || item.phase == "reverted" {
				present = true // A canonical finalized receipt consumed this nonce.
				break
			}
			hash := common.HexToHash(item.hash)
			if _, err = r.chain.InclusionReceipt(ctx, hash); err == nil {
				present = true
				break
			} else if !errors.Is(err, ethereum.NotFound) {
				return allocated, err
			}
			if _, _, err = r.TransactionByHash(ctx, hash); err == nil {
				present = true
				break
			} else if !errors.Is(err, ethereum.NotFound) {
				return allocated, err
			}
		}
		if present {
			continue
		}
		if len(identities) == 0 {
			return allocated, fmt.Errorf("missing saved redemption at nonce %d; restore its signed transaction before submitting more", pending)
		}
		item := identities[0]
		r.mu.Lock()
		active := r.tracking[common.HexToHash(item.root)]
		r.mu.Unlock()
		if active {
			return allocated, errRecoveringNonce // The existing timeout/replacement lifecycle owns it.
		}
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(item.raw); err != nil {
			return allocated, errors.New("invalid stored redemption transaction")
		}
		identity, err := signedIdentity(tx)
		if err != nil || identity.Hash.Hex() != item.hash || identity.From != address || identity.Nonce != pending {
			return allocated, errors.New("stored redemption identity does not match its signed bytes")
		}
		if err := broadcastSavedRedemption(ctx, r.store, r.chain.Contracts, item.sig, identity, true); err != nil {
			return allocated, err
		}
		return allocated, errRecoveringNonce
	}
	return allocated, nil
}

// A finalized account count can resolve a missing local hash consumed by another
// process. It cannot establish ticket success, or resolve an unfinalized receipt.
func (s *RedeemerDB) reconcileFinalizedNonces(ctx context.Context, chain finalizedNonceReader, pending map[common.Hash]bool) []error {
	if len(pending) == 0 {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT t.hash,t.redeemer_address,t.nonce FROM redemption_transactions t JOIN redemption_attempts a ON a.sig=t.sig WHERE a.phase NOT IN ('confirmed','used') AND t.phase NOT IN ('confirmed','reverted','superseded')`)
	if err != nil {
		return []error{err}
	}
	type unresolved struct {
		hash  common.Hash
		nonce uint64
	}
	bySender := map[common.Address][]unresolved{}
	for rows.Next() {
		var hash, sender, nonce string
		if err = rows.Scan(&hash, &sender, &nonce); err != nil {
			break
		}
		if !pending[common.HexToHash(hash)] {
			continue
		}
		var n uint64
		n, err = strconv.ParseUint(nonce, 10, 64)
		if err != nil || !common.IsHexAddress(sender) {
			err = errors.New("invalid stored redemption account or nonce")
			break
		}
		address := common.HexToAddress(sender)
		bySender[address] = append(bySender[address], unresolved{common.HexToHash(hash), n})
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return []error{err}
	}
	var failures []error
	for address, items := range bySender {
		count, err := chain.FinalizedNonce(ctx, address)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		for _, item := range items {
			if item.nonce >= count {
				continue
			}
			// Receipt's boolean result includes unfinalized inclusion. Only an
			// actual missing/orphaned receipt permits external-nonce retirement.
			if _, err := chain.InclusionReceipt(ctx, item.hash); !errors.Is(err, ethereum.NotFound) {
				if err != nil {
					failures = append(failures, err)
				}
				continue
			}
			tx, err := s.db.BeginTx(ctx, nil)
			if err == nil {
				_, err = tx.ExecContext(ctx, `UPDATE redemption_transactions SET phase='superseded',error=NULL WHERE hash=? AND phase NOT IN ('confirmed','reverted','superseded')`, item.hash.Hex())
				if err == nil {
					_, err = tx.ExecContext(ctx, `UPDATE redemption_attempts SET phase='retryable',error=NULL WHERE phase IN ('prepared','broadcast','submitted','mined') AND sig IN (SELECT sig FROM winning_tickets WHERE tx_hash=?)`, item.hash.Hex())
				}
				if err == nil {
					err = tx.Commit()
				}
				tx.Rollback()
			}
			if err != nil {
				failures = append(failures, err)
			}
		}
	}
	return failures
}
