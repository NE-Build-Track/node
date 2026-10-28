// Ticket queue/retry policy ported from go-livepeer/pm/queue.go and
// pm/sendermonitor.go at 0ff08af1bb4cb268f428d619cc968b5eaee0d093,
// by Yondon Fu, Nico Vergauwen and Rafal Leszko. MIT notice in LICENSE.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/pm"
)

// Used tickets and finalized failures settle through their chain observations.
// This is the terminal error that can arise during submission itself.
func isNonRetryableTicketErr(err error) bool {
	return errors.Is(err, eth.ErrTicketCreationRound)
}

// RedeemPending adapts handleBlockEvent and redeemWinningTicket to node snapshots.
// Inclusion/replacement waits run on the service manager, never on this heartbeat.
func RedeemPending(ctx context.Context, store *RedeemerDB, chain eth.PaymentChain, key *eth.Key, chainID *big.Int, snapshot eth.ChainSnapshot) []error {
	if store == nil || key == nil || chainID == nil || chain.Contracts == nil || snapshot.Block == nil || snapshot.Round == nil {
		return []error{errors.New("redemption is not configured")}
	}
	// The fallback manager is store-owned and joined by Close. Production starts
	// it first under the service context with its configured recovery options.
	recovery, err := store.recoveryFor(context.WithoutCancel(ctx), chain, key, defaultRedemptionOptions)
	if err != nil {
		return []error{err}
	}
	failures := recovery.resume(ctx)
	payers, err := store.PendingPayers()
	if err != nil {
		return append(failures, err)
	}
	for _, payer := range payers {
		if ctx.Err() != nil {
			return append(failures, ctx.Err())
		}
		ticket, err := store.SelectEarliestWinningTicket(payer, 0)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if ticket == nil {
			continue
		}
		if ticket.CreationRound < snapshot.Round.Int64()-2 {
			err = store.setTicketPhase(ctx, ticket.Sig, "expired")
			if err != nil {
				failures = append(failures, err)
			}
			continue
		}
		if ticket.ParamsExpirationBlock.Cmp(snapshot.Block) > 0 {
			continue
		}
		active, err := chain.IsActiveAt(ctx, ticket.Recipient, snapshot)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if !active {
			continue
		}
		used, err := chain.IsUsedTicket(ctx, ticket.Hash(), "latest")
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if used {
			finalized, err := chain.IsUsedTicket(ctx, ticket.Hash(), "finalized")
			if err == nil {
				if finalized {
					err = store.setTicketPhase(ctx, ticket.Sig, "used")
				} else {
					err = store.setTicketPhase(ctx, ticket.Sig, "awaiting_finality")
				}
			}
			if err != nil {
				failures = append(failures, err)
			}
			continue
		}
		err = chain.SubmitRedemption(ctx, recovery.nonces, recovery.manager, key, chainID, redeemTicket(ticket), func(tx eth.SignedTransaction) error { return store.recordPrepared(ctx, ticket, tx) })
		if errors.Is(err, errRecoveringNonce) {
			continue
		}
		if err != nil {
			if isNonRetryableTicketErr(err) {
				err = errors.Join(err, store.setTicketPhase(ctx, ticket.Sig, "invalid"))
			}
			failures = append(failures, fmt.Errorf("payer %s: %w", payer.Hex(), err))
		}
	}
	return failures
}
func redeemTicket(ticket *pm.SignedTicket) eth.RedeemTicket {
	return eth.RedeemTicket{Recipient: ticket.Recipient, Sender: ticket.PayerAddress, FaceValue: ticket.FaceValue, WinProb: ticket.WinProb, SenderNonce: ticket.TicketNonce, RecipientRandHash: ticket.RecipientRandHash, AuxData: ticket.AuxData(), Signature: ticket.Sig, RecipientRand: ticket.RecipientRand}
}
func (s *RedeemerDB) setTicketPhase(ctx context.Context, sig []byte, phase string) error {
	if terminalRedemption(phase) {
		return s.finishTicket(ctx, sig, phase, "")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO redemption_attempts(sig,attempted_at,phase) VALUES(?,?,?) ON CONFLICT(sig) DO UPDATE SET phase=excluded.phase WHERE redemption_attempts.phase IN ('retryable','awaiting_finality') AND redemption_attempts.phase!=excluded.phase`, sig, time.Now().UTC().Format(time.RFC3339Nano), phase)
	if err == nil {
		if n, _ := result.RowsAffected(); n > 0 {
			s.observeTransition(phase, "", nil)
		}
	}
	return err
}

type ReceiptReader interface {
	Receipt(context.Context, common.Hash) (confirmed, reverted bool, err error)
}

// ReconcileSubmitted checks every retained identity, including old retry groups.
// A finalized success wins over sibling failures; failed attempts alone cannot
// release liability while another signed identity might still redeem the ticket.
func ReconcileSubmitted(ctx context.Context, store *RedeemerDB, chain ReceiptReader) []error {
	items, err := store.SubmittedRedemptions()
	if err != nil {
		return []error{err}
	}
	var failures []error
	successes := map[string]bool{}
	reverts := map[string]common.Hash{}
	pending := map[common.Hash]bool{}
	for _, item := range items {
		confirmed, reverted, err := chain.Receipt(ctx, item.Hash)
		id := string(item.Signature)
		if err != nil {
			failures = append(failures, fmt.Errorf("receipt %s: %w", item.Hash.Hex(), err))
			continue
		}
		if confirmed {
			successes[id] = true
			err = store.finishTicket(ctx, item.Signature, "confirmed", item.Hash.Hex())
		} else if reverted {
			reverts[id] = item.Hash
			err = store.finishTicket(ctx, item.Signature, "reverted", item.Hash.Hex())
		} else {
			pending[item.Hash] = true
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	for id, hash := range reverts {
		if successes[id] {
			continue
		}
		failures = append(failures, fmt.Errorf("transaction failed: redemption %s reverted", hash.Hex()))
	}
	if reader, ok := chain.(finalizedNonceReader); ok {
		failures = append(failures, store.reconcileFinalizedNonces(ctx, reader, pending)...)
	}
	return failures
}
func ProcessRedemptions(ctx context.Context, store *RedeemerDB, chain eth.PaymentChain, key *eth.Key, chainID *big.Int) []error {
	if store == nil || key == nil {
		return nil
	}
	// Reconcile before admission so a successful predecessor prevents resubmission.
	failures := ReconcileSubmitted(ctx, store, chain)
	snapshot, err := chain.Snapshot(ctx)
	if err != nil {
		return append(failures, err)
	}
	return append(failures, RedeemPending(ctx, store, chain, key, chainID, snapshot)...)
}
