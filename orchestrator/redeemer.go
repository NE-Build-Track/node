package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
)

// RedeemPending only reveals randomness once the parameter acceptance window
// has ended. Preparation failures leave tickets queued; signed identities are
// durable before broadcasting. Call from one bounded worker per store.
// The expiry rule follows Nico Vergauwen's go-livepeer/pm/queue.go; L1-clock
// handling follows Rafał Leszko's 4b6ede31f040a084ff9e55bd798a0d6fffee1e1b.
func RedeemPending(ctx context.Context, store *RedeemerDB, chain eth.PaymentChain, redeemer *eth.Key, chainID *big.Int, snapshot eth.ChainSnapshot) []error {
	if store == nil || redeemer == nil || chainID == nil || snapshot.Block == nil || snapshot.Round == nil {
		return []error{fmt.Errorf("redemption is not configured")}
	}
	// A crash after preparation but before the broadcast marker is safe to
	// resume. A broadcast marker requires receipt checks or explicit retry.
	var preparedHash string
	err := store.db.QueryRow("SELECT w.tx_hash FROM winning_tickets w JOIN redemption_attempts a ON a.sig=w.sig WHERE a.phase='prepared' LIMIT 1").Scan(&preparedHash)
	if err == nil {
		if err := RetryRedemption(ctx, store, chain.Contracts, ethcommon.HexToHash(preparedHash)); err != nil {
			return []error{err}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return []error{err}
	}
	var blocked int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM redemption_attempts WHERE phase IN ('prepared','broadcast')").Scan(&blocked); err != nil {
		return []error{err}
	}
	if blocked != 0 {
		return nil
	}
	floor, err := store.SavedNonceFloor(ctx, redeemer.Address())
	if err != nil {
		return []error{err}
	}
	payers, err := store.PendingPayers()
	if err != nil {
		return []error{err}
	}
	var failures []error
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
			_, err := store.db.Exec("INSERT INTO redemption_attempts(sig,attempted_at,phase,error) VALUES(?,?,'expired','ticket creation round expired')", ticket.Sig, time.Now().UTC().Format(time.RFC3339Nano))
			if err != nil {
				failures = append(failures, err)
			}
			continue
		}
		if ticket.ParamsExpirationBlock.Cmp(snapshot.Block) > 0 {
			continue
		}
		redeemTicket := eth.RedeemTicket{Recipient: ticket.Recipient, Sender: ticket.PayerAddress, FaceValue: ticket.FaceValue, WinProb: ticket.WinProb, SenderNonce: ticket.TicketNonce, RecipientRandHash: ticket.RecipientRandHash, AuxData: ticket.AuxData(), Signature: ticket.Sig, RecipientRand: ticket.RecipientRand}
		if chain.Contracts == nil {
			return append(failures, errors.New("redemption is not configured"))
		}
		chain.Contracts.SetNonceFloor(redeemer.Address(), floor)
		prepared, err := chain.PrepareRedemptionAndStore(ctx, redeemer, chainID, redeemTicket, func(prepared eth.SignedTransaction) error {
			return store.recordPrepared(ctx, ticket, prepared)
		})
		if err == nil {
			err = broadcastRedemption(ctx, store, chain.Contracts, ticket.Sig, prepared)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("payer %s: %w", payer.Hex(), err))
		}
	}
	return failures
}

type ReceiptReader interface {
	Receipt(context.Context, ethcommon.Hash) (confirmed, reverted bool, err error)
}

// ReconcileSubmitted checks receipts once per background tick. It never
// broadcasts, so a pending or uncertain transaction cannot be sent twice.
func ReconcileSubmitted(ctx context.Context, store *RedeemerDB, chain ReceiptReader) []error {
	items, err := store.SubmittedRedemptions()
	if err != nil {
		return []error{err}
	}
	var failures []error
	for _, item := range items {
		confirmed, reverted, err := chain.Receipt(ctx, item.Hash)
		if err != nil {
			failures = append(failures, fmt.Errorf("receipt %s: %w", item.Hash.Hex(), err))
			continue
		}
		if confirmed {
			err = store.ConfirmRedemption(item.Signature)
		}
		if reverted {
			err = store.FailRedemption(item.Signature)
			failures = append(failures, fmt.Errorf("transaction %s reverted", item.Hash.Hex()))
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return failures
}

func ProcessRedemptions(ctx context.Context, store *RedeemerDB, chain eth.PaymentChain, redeemer *eth.Key, chainID *big.Int) []error {
	var failures []error
	if store == nil || redeemer == nil {
		return nil
	}
	snapshot, err := chain.Snapshot(ctx)
	if err != nil {
		failures = append(failures, err)
	} else {
		active, err := chain.IsActiveAt(ctx, redeemer.Address(), snapshot)
		if err != nil {
			failures = append(failures, err)
		} else {
			if active {
				failures = append(failures, RedeemPending(ctx, store, chain, redeemer, chainID, snapshot)...)
			}
		}
	}
	failures = append(failures, ReconcileSubmitted(ctx, store, chain)...)
	return failures
}
