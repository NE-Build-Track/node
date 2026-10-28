package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/livepeer/node/internal/observability"
)

// observeTransition is called only after a durable change commits. Callback
// failures are counted here because they do not necessarily reach the poller.
func (s *RedeemerDB) observeTransition(phase, hash string, err error) {
	s.metrics.Add("redemption_transitions_total", 1, observability.Labels{"event": phase})
	if err != nil {
		s.metrics.Add("redemption_errors_total", 1, observability.Labels{"error_kind": observability.ErrorKind(err)})
	}
	if s.logger != nil {
		s.logger.Info("redemption state changed", "event", "redemption_transition", "operation", "redeem", "outcome", phase, "transaction_hash", hash, "error_kind", observability.ErrorKind(err))
	}
}

// collectMetrics uses a read transaction so tickets and replacement identities
// describe one durable snapshot. The caller never probes the chain.
func (s *RedeemerDB) collectMetrics(emit observability.Emit) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stats, err := s.redemptionStats(ctx)
	emit("collector_healthy", observability.Bool(err == nil), observability.Labels{"collector": "redemptions"})
	if err != nil {
		return
	}
	for state, count := range stats.tickets {
		emit("redemption_queue_tickets", float64(count), observability.Labels{"state": state})
	}
	for state, at := range stats.oldestTickets {
		emit("redemption_oldest_ticket_timestamp_seconds", at, observability.Labels{"state": state})
	}
	for phase, count := range stats.transactions {
		emit("redemption_unresolved_transactions", float64(count), observability.Labels{"phase": phase})
	}
	for phase, at := range stats.oldestTransactions {
		emit("redemption_oldest_transaction_timestamp_seconds", at, observability.Labels{"phase": phase})
	}
	emit("redemption_last_settlement_timestamp_seconds", stats.lastSettlement, nil)
}

type redemptionStats struct {
	tickets, transactions             map[string]int
	oldestTickets, oldestTransactions map[string]float64
	lastSettlement                    float64
}

func (s *RedeemerDB) redemptionStats(ctx context.Context) (stats redemptionStats, err error) {
	stats = redemptionStats{tickets: map[string]int{"waiting": 0, "eligible": 0, "in_flight": 0, "awaiting_finality": 0, "unknown": 0}, transactions: map[string]int{}, oldestTickets: map[string]float64{}, oldestTransactions: map[string]float64{}}
	for state := range stats.tickets {
		stats.oldestTickets[state] = 0
	}
	for _, phase := range []string{"prepared", "broadcast", "submitted", "mined", "retryable", "used", "expired", "invalid", "unknown"} {
		stats.transactions[phase] = 0
		stats.oldestTransactions[phase] = 0
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return stats, err
	}
	defer tx.Rollback()
	// A cached protocol snapshot only determines clock eligibility; unavailable
	// or stale clocks are reported as unknown instead of fabricating zero work.
	var block, round *big.Int
	if s.readiness != nil {
		s.readiness.mu.Lock()
		if time.Since(s.readiness.completed) <= max(time.Minute, 3*s.readiness.interval) && s.readiness.chain {
			block, round = s.readiness.snapshot.Block, s.readiness.snapshot.Round
		}
		s.readiness.mu.Unlock()
	}
	rows, err := tx.QueryContext(ctx, `SELECT COALESCE(w.enqueued_at,''),w.params_expiration_block,w.creation_round,COALESCE(a.phase,'') FROM winning_tickets w LEFT JOIN redemption_attempts a ON a.sig=w.sig WHERE w.redeemed_at IS NULL AND COALESCE(a.phase,'') NOT IN ('confirmed','used','expired','invalid','reverted')`)
	if err != nil {
		return stats, err
	}
	for rows.Next() {
		var at, expiry, phase string
		var creation int64
		if err = rows.Scan(&at, &expiry, &creation, &phase); err != nil {
			break
		}
		state := "unknown"
		switch phase {
		case "prepared", "broadcast", "sending", "submitted", "mined":
			state = "in_flight"
		case "awaiting_finality":
			state = "awaiting_finality"
		default:
			if block != nil && round != nil {
				end, ok := new(big.Int).SetString(expiry, 10)
				if !ok {
					rows.Close()
					return stats, fmt.Errorf("invalid ticket expiry")
				}
				if creation < round.Int64()-2 {
					continue
				}
				state = "eligible"
				if end.Cmp(block) > 0 {
					state = "waiting"
				}
			}
		}
		stats.tickets[state]++
		if err = recordOldest(stats.oldestTickets, state, at); err != nil {
			break
		}
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return stats, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT t.phase,t.prepared_at FROM redemption_transactions t JOIN redemption_attempts a ON a.sig=t.sig WHERE a.phase NOT IN ('confirmed','used') AND t.phase NOT IN ('confirmed','reverted','superseded')`)
	if err != nil {
		return stats, err
	}
	for rows.Next() {
		var phase, at string
		if err = rows.Scan(&phase, &at); err != nil {
			break
		}
		if phase == "sending" {
			phase = "broadcast"
		}
		switch phase {
		case "prepared", "broadcast", "submitted", "mined", "retryable", "used", "expired", "invalid":
		default:
			phase = "unknown"
		}
		stats.transactions[phase]++
		if err = recordOldest(stats.oldestTransactions, phase, at); err != nil {
			break
		}
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return stats, err
	}
	var last sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT MAX(redeemed_at) FROM winning_tickets`).Scan(&last); err != nil {
		return stats, err
	}
	if last.Valid {
		var at time.Time
		at, err = time.Parse(time.RFC3339Nano, last.String)
		stats.lastSettlement = float64(at.Unix())
	}
	return stats, err
}

func recordOldest(values map[string]float64, key, raw string) error {
	if raw == "" {
		return nil
	}
	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return err
	}
	n := float64(at.Unix())
	if values[key] == 0 || n < values[key] {
		values[key] = n
	}
	return nil
}
