package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
)

// SavedNonceFloor restores only acknowledged sends, following upstream backend's
// Update-after-send rule. A rejected/prepared identity must not create a gap.
func (s *RedeemerDB) SavedNonceFloor(ctx context.Context, address ethcommon.Address) (uint64, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT nonce FROM redemption_transactions WHERE redeemer_address=? AND acknowledged=1 ORDER BY length(nonce) DESC,nonce DESC LIMIT 1`, address.Hex()).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || n == math.MaxUint64 {
		return 0, errors.New("invalid or exhausted saved redemption nonce")
	}
	return n + 1, nil
}

func (s *RedeemerDB) recordPrepared(ctx context.Context, ticket *pm.SignedTicket, prepared eth.SignedTransaction) error {
	return s.recordTransaction(ctx, ticket.Sig, prepared, "")
}

func (s *RedeemerDB) recordTransaction(ctx context.Context, sig []byte, prepared eth.SignedTransaction, previous string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	root := prepared.Hash.Hex()
	if previous != "" {
		if err := tx.QueryRowContext(ctx, "SELECT root_hash FROM redemption_transactions WHERE hash=? AND sig=?", previous, sig).Scan(&root); err != nil {
			return err
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var result sql.Result
	if previous == "" {
		result, err = tx.ExecContext(ctx, `INSERT INTO redemption_attempts(sig,attempted_at,phase,raw_transaction,redeemer_address,nonce)
   VALUES(?,?,'prepared',?,?,?) ON CONFLICT(sig) DO UPDATE SET attempted_at=excluded.attempted_at,phase='prepared',error=NULL,raw_transaction=excluded.raw_transaction,redeemer_address=excluded.redeemer_address,nonce=excluded.nonce WHERE redemption_attempts.phase='retryable'`, sig, now, prepared.Raw, prepared.From.Hex(), strconv.FormatUint(prepared.Nonce, 10))
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE redemption_attempts SET attempted_at=?,phase='prepared',error=NULL,raw_transaction=?,redeemer_address=?,nonce=? WHERE sig=? AND phase IN ('prepared','broadcast','submitted','mined') AND EXISTS(SELECT 1 FROM winning_tickets WHERE sig=? AND tx_hash=?)`, now, prepared.Raw, prepared.From.Hex(), strconv.FormatUint(prepared.Nonce, 10), sig, sig, previous)
	}
	if err != nil {
		return err
	}
	if n, e := result.RowsAffected(); e != nil || n != 1 {
		return errors.New("redemption is already active or resolved")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO redemption_transactions(hash,sig,root_hash,replaces_hash,raw_transaction,redeemer_address,nonce,phase,prepared_at) VALUES(?,?,?,?,?,?,?,'prepared',?) ON CONFLICT(hash) DO UPDATE SET phase='prepared',error=NULL`, prepared.Hash.Hex(), sig, root, nullString(previous), prepared.Raw, prepared.From.Hex(), strconv.FormatUint(prepared.Nonce, 10), now)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE winning_tickets SET tx_hash=? WHERE sig=?", prepared.Hash.Hex(), sig)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	event := "prepared"
	if previous != "" {
		event = "replacement_prepared"
	}
	s.observeTransition(event, prepared.Hash.Hex(), nil)
	return nil
}
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// RedemptionStatus excludes recipient randomness, signatures and signed bytes.
type RedemptionStatus struct {
	Hash  string `json:"hash,omitempty"`
	Phase string `json:"phase"`
	Error string `json:"error,omitempty"`
}

func (s *RedeemerDB) Redemptions() ([]RedemptionStatus, error) {
	rows, err := s.db.Query(`SELECT hash,phase,error FROM (
 SELECT hash,CASE WHEN phase='sending' THEN 'broadcast' ELSE phase END AS phase,COALESCE(error,'') AS error,rowid AS sequence FROM redemption_transactions
 UNION ALL SELECT '',phase,COALESCE(error,''),9223372036854775807 FROM redemption_attempts a WHERE a.phase IN ('used','awaiting_finality') OR NOT EXISTS(SELECT 1 FROM redemption_transactions t WHERE t.sig=a.sig)) ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RedemptionStatus{}
	for rows.Next() {
		var item RedemptionStatus
		if err := rows.Scan(&item.Hash, &item.Phase, &item.Error); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// RetryRedemption preserves the existing explicit identical-byte retry command.
// Historical identities are inspectable, but cannot replace a newer active job.
func RetryRedemption(ctx context.Context, store *RedeemerDB, contracts *eth.Contracts, hash ethcommon.Hash) error {
	var sig, raw []byte
	err := store.db.QueryRowContext(ctx, `SELECT t.sig,t.raw_transaction FROM redemption_transactions t JOIN redemption_attempts a ON a.sig=t.sig JOIN winning_tickets w ON w.sig=t.sig WHERE t.hash=? AND w.tx_hash=t.hash AND a.phase IN ('prepared','broadcast','submitted','retryable') AND t.phase IN ('prepared','broadcast','submitted','retryable')`, hash.Hex()).Scan(&sig, &raw)
	if err != nil {
		return fmt.Errorf("redemption has no retryable signed transaction: %w", err)
	}
	return broadcastRedemption(ctx, store, contracts, sig, eth.SignedTransaction{Hash: hash, Raw: raw})
}

func broadcastRedemption(ctx context.Context, store *RedeemerDB, contracts *eth.Contracts, sig []byte, identity eth.SignedTransaction) error {
	return broadcastSavedRedemption(ctx, store, contracts, sig, identity, false)
}

// Gap repair may replay older signed bytes without displacing current work or
// reopening a terminal ticket. Manual retries retain their current-only guard.
func broadcastSavedRedemption(ctx context.Context, store *RedeemerDB, contracts *eth.Contracts, sig []byte, identity eth.SignedTransaction, historical bool) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Claim the send atomically, including against another manual retry. A crash
	// in 'sending' is recovered as an uncertain send using the same stored bytes.
	var previousPhase string
	if err := tx.QueryRowContext(ctx, "SELECT phase FROM redemption_transactions WHERE hash=? AND sig=?", identity.Hash.Hex(), sig).Scan(&previousPhase); err != nil {
		return err
	}
	// Closed histories cannot be manually retried. Keep their recorded outcome
	// durable during replay as well: a crash must not replace it with 'sending'.
	preserve := historical && (terminalRedemption(previousPhase) || previousPhase == "superseded")
	claimedPhase := "sending"
	if preserve {
		claimedPhase = previousPhase
	}
	result, err := tx.ExecContext(ctx, `UPDATE redemption_transactions SET phase=?,error=NULL,broadcast_at=CASE WHEN NOT ? AND phase IN ('prepared','retryable') THEN ? ELSE COALESCE(broadcast_at,?) END WHERE hash=? AND sig=? AND (phase IN ('prepared','broadcast','submitted','retryable') OR (? AND phase IN ('sending','mined','used','expired','invalid','superseded'))) AND EXISTS(SELECT 1 FROM redemption_attempts a JOIN winning_tickets w ON w.sig=a.sig WHERE a.sig=? AND (? OR (w.tx_hash=? AND a.phase IN ('prepared','broadcast','submitted','retryable'))))`, claimedPhase, historical, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano), identity.Hash.Hex(), sig, historical, sig, historical, identity.Hash.Hex())
	if err != nil {
		return err
	}
	if n, e := result.RowsAffected(); e != nil || n != 1 {
		return errors.New("redemption is already sending, mined or resolved")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE redemption_attempts SET phase='broadcast',error=NULL WHERE sig=? AND phase IN ('prepared','broadcast','submitted','retryable','mined') AND EXISTS(SELECT 1 FROM winning_tickets WHERE sig=? AND tx_hash=?)`, sig, sig, identity.Hash.Hex()); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	// An acknowledged replay does not begin a new logical broadcast phase.
	broadcastChanged := !preserve && previousPhase != "sending" && previousPhase != "broadcast" && previousPhase != "submitted"
	if broadcastChanged {
		store.observeTransition("broadcast", identity.Hash.Hex(), nil)
	}
	sendErr := contracts.Broadcast(ctx, identity)
	phase := "submitted"
	var message any
	ack := 0
	if sendErr != nil {
		phase = "retryable"
		// Resending acknowledged bytes does not undo their live monitor or
		// acknowledgment, even if this particular resend is rejected.
		if previousPhase == "submitted" {
			phase = "submitted"
		}
		message = sendErr.Error()
	} else {
		ack = 1
	}
	if preserve {
		phase = previousPhase
	}
	tx, err = store.db.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		return errors.Join(sendErr, err)
	}
	defer tx.Rollback()
	result, err = tx.Exec(`UPDATE redemption_transactions SET phase=?,error=?,acknowledged=max(acknowledged,?) WHERE hash=? AND phase=?`, phase, message, ack, identity.Hash.Hex(), claimedPhase)
	var summaryResult sql.Result
	if err == nil {
		summaryResult, err = tx.Exec(`UPDATE redemption_attempts SET phase=?,error=? WHERE sig=? AND phase='broadcast' AND EXISTS(SELECT 1 FROM winning_tickets WHERE sig=? AND tx_hash=?)`, phase, message, sig, sig, identity.Hash.Hex())
	}
	if err != nil {
		return errors.Join(sendErr, err)
	}
	commitErr := tx.Commit()
	if commitErr == nil {
		store.metrics.Add("redemption_broadcasts_total", 1, observability.Labels{"outcome": observability.ErrorKind(sendErr)})
		changed, _ := result.RowsAffected()
		summaryChanged, _ := summaryResult.RowsAffected()
		if changed+summaryChanged > 0 && !preserve && (broadcastChanged || phase != previousPhase) {
			// Broadcast errors are observed by the caller; this records the phase.
			store.observeTransition(phase, identity.Hash.Hex(), nil)
		}
	}
	return errors.Join(sendErr, commitErr)
}

func signedIdentity(tx *types.Transaction) (eth.SignedTransaction, error) {
	raw, err := tx.MarshalBinary()
	if err != nil {
		return eth.SignedTransaction{}, err
	}
	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		return eth.SignedTransaction{}, err
	}
	return eth.SignedTransaction{Hash: tx.Hash(), Raw: raw, Nonce: tx.Nonce(), From: from}, nil
}

// finishTicket never discards transaction history or the epoch exposure marker.
func (s *RedeemerDB) finishTicket(ctx context.Context, sig []byte, phase string, hash string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var retried []string
	commit := func() error {
		if err := tx.Commit(); err != nil {
			return err
		}
		for _, hash := range retried {
			s.observeTransition("retryable", hash, nil)
		}
		return nil
	}
	var oldPhase, enqueued string
	var face []byte
	var redeemed bool
	var observationErr error
	if s.metrics != nil {
		observationErr = tx.QueryRowContext(ctx, `SELECT COALESCE(a.phase,''),COALESCE(w.enqueued_at,''),w.face_value,w.redeemed_at IS NOT NULL FROM winning_tickets w LEFT JOIN redemption_attempts a ON a.sig=w.sig WHERE w.sig=?`, sig).Scan(&oldPhase, &enqueued, &face, &redeemed)
	}
	if hash == "" && phase != "used" {
		var latest sql.NullString
		if err := tx.QueryRowContext(ctx, "SELECT tx_hash FROM winning_tickets WHERE sig=?", sig).Scan(&latest); err != nil {
			return err
		}
		hash = latest.String
	}
	if phase == "confirmed" || phase == "reverted" {
		// Every finalized receipt consumes the sender's nonce across tickets and
		// retry groups. Other nonces remain uncertain. Retire identities before
		// applying the ticket outcome, which may already be a finalized success.
		_, err = tx.ExecContext(ctx, `UPDATE redemption_transactions SET phase='superseded',error=NULL WHERE hash!=? AND (redeemer_address,nonce)=(SELECT redeemer_address,nonce FROM redemption_transactions WHERE hash=?) AND phase NOT IN ('confirmed','reverted')`, hash, hash)
		if err == nil {
			var rows *sql.Rows
			rows, err = tx.QueryContext(ctx, `UPDATE redemption_attempts SET phase='retryable',error=NULL WHERE sig!=? AND phase IN ('prepared','broadcast','submitted','mined') AND EXISTS(SELECT 1 FROM winning_tickets w JOIN redemption_transactions t ON t.hash=w.tx_hash WHERE w.sig=redemption_attempts.sig AND t.phase='superseded' AND (t.redeemer_address,t.nonce)=(SELECT redeemer_address,nonce FROM redemption_transactions WHERE hash=?)) RETURNING (SELECT tx_hash FROM winning_tickets WHERE sig=redemption_attempts.sig)`, sig, hash)
			if err == nil {
				for rows.Next() {
					var sibling sql.NullString
					if err = rows.Scan(&sibling); err != nil {
						break
					}
					retried = append(retried, sibling.String)
				}
				if err == nil {
					err = rows.Err()
				}
				rows.Close()
			}
		}
		var message any
		if phase == "reverted" {
			message = "transaction reverted"
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE redemption_transactions SET phase=?,error=?,acknowledged=1 WHERE hash=?`, phase, message, hash)
		}
		if err != nil {
			return err
		}
	}
	// Creating the summary and settling a terminal outcome must commit together.
	result, err := tx.ExecContext(ctx, `INSERT INTO redemption_attempts(sig,attempted_at,phase) VALUES(?,?,?) ON CONFLICT(sig) DO UPDATE SET phase=excluded.phase,error=NULL WHERE redemption_attempts.phase NOT IN ('confirmed','used') OR excluded.phase='confirmed' OR (excluded.phase='used' AND redemption_attempts.phase!='confirmed')`, sig, time.Now().UTC().Format(time.RFC3339Nano), phase)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return commit()
	}
	if phase == "confirmed" || phase == "used" {
		_, err = tx.ExecContext(ctx, `UPDATE winning_tickets SET redeemed_at=COALESCE(redeemed_at,?),tx_hash=COALESCE(?,tx_hash) WHERE sig=?`, time.Now().UTC().Format(time.RFC3339Nano), nullString(hash), sig)
	}
	if err == nil && (phase == "expired" || phase == "invalid") {
		// Submission eligibility can end before an uncertain send finalizes.
		// Keep every predecessor receipt-visible until actual settlement.
		_, err = tx.ExecContext(ctx, `UPDATE redemption_transactions SET phase=?,error=NULL WHERE hash=? AND phase NOT IN ('confirmed','reverted','superseded')`, phase, hash)
	} else if err == nil && phase != "reverted" {
		_, err = tx.ExecContext(ctx, `UPDATE redemption_transactions SET phase=CASE WHEN hash=? THEN ? ELSE 'superseded' END,error=NULL,acknowledged=CASE WHEN hash=? AND ?='confirmed' THEN 1 ELSE acknowledged END WHERE sig=? AND phase NOT IN ('confirmed','reverted')`, hash, phase, hash, phase, sig)
	}
	if err != nil {
		return err
	}
	if err := commit(); err != nil {
		return err
	}
	if s.metrics != nil && observationErr == nil {
		if oldPhase != phase {
			s.observeTransition(phase, hash, nil)
		}
		if !redeemed && (phase == "confirmed" || phase == "used") {
			s.metrics.Add("redemption_settlements_total", 1, nil)
			s.metrics.Add("redemption_settled_eth_total", observability.ETH(new(big.Rat).SetInt(new(big.Int).SetBytes(face))), nil)
			if at, err := time.Parse(time.RFC3339Nano, enqueued); err == nil {
				s.metrics.Observe("redemption_settlement_duration_seconds", max(time.Since(at).Seconds(), 0), nil)
			}
		}
	}
	return nil
}
