package orchestrator

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

func TestDroppedRedemptionRefillsNonceAfterRestart(t *testing.T) {
	for _, phase := range []string{"retryable", "expired", "invalid", "used"} {
		t.Run(phase, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 230)
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: 0}))
			tickRecovery(t, s, c, key)
			original := f.transactions()[0]
			f.mu.Lock()
			delete(f.pool, 0)
			f.mu.Unlock()
			require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "retryable" }, time.Second, time.Millisecond)
			s = reopenRecoveryStore(t, s)
			if phase != "retryable" {
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, phase))
			}
			// A failed claim must prevent both the saved-byte replay and any fresh
			// higher nonce. Restart retains the original timestamp and identity.
			_, err := s.db.Exec(`CREATE TRIGGER reject_gap_send BEFORE UPDATE OF phase ON redemption_transactions BEGIN SELECT RAISE(ABORT,'injected persistence failure'); END`)
			require.NoError(t, err)
			require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			require.Len(t, f.transactions(), 1)
			_, err = s.db.Exec("DROP TRIGGER reject_gap_send")
			require.NoError(t, err)
			tickRecovery(t, s, c, key)
			sent := f.transactions()
			require.Len(t, sent, 2)
			require.Equal(t, original.Hash(), sent[1].Hash(), "refill the evicted slot with the exact saved bytes")
			if phase != "retryable" {
				require.Equal(t, phase, ticketPhase(t, s, ticket.Sig), "gap repair must not reopen terminal tickets")
			}
			f.mine(sent[1], true)
			tickRecovery(t, s, c, key)
			next := nextRecoveryTicket(ticket, 231)
			require.NoError(t, s.StoreWinningTicket(next))
			f.mu.Lock()
			f.tickets = append(f.tickets, next)
			f.mu.Unlock()
			tickRecovery(t, s, c, key)
			sent = f.transactions()
			require.Len(t, sent, 3)
			require.EqualValues(t, 1, sent[2].Nonce())
			f.mine(sent[2], true)
			tickRecovery(t, s, c, key)
			require.Equal(t, "confirmed", ticketPhase(t, s, next.Sig), "queued work must actually settle after the gap is repaired")
		})
	}
}

func TestClosedTicketRepairsSupersededHigherNonce(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 232)
	// The original send lost its response; a fresh attempt uses nonce one, as
	// required by the retry policy. Either attempt can later win.
	f.loseNextResponse = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	tickRecovery(t, s, c, key)
	s = reopenRecoveryStore(t, s)
	sent := f.transactions()
	require.Len(t, sent, 2)
	require.EqualValues(t, 0, sent[0].Nonce())
	require.EqualValues(t, 1, sent[1].Nonce())
	f.mine(sent[0], true)
	require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
	f.mu.Lock()
	delete(f.pool, 1)
	f.finalizedNonce = 1
	f.mu.Unlock()
	// Crash after replay submission but before recording its acknowledgment.
	_, err := s.db.Exec(`CREATE TRIGGER reject_gap_ack BEFORE UPDATE OF acknowledged ON redemption_transactions BEGIN SELECT RAISE(ABORT,'injected acknowledgment failure'); END`)
	require.NoError(t, err)
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	s = reopenRecoveryStore(t, s)
	var interrupted string
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", sent[1].Hash().Hex()).Scan(&interrupted))
	require.Equal(t, "superseded", interrupted, "the recorded historical outcome must survive interrupted replay")
	_, err = s.db.Exec("DROP TRIGGER reject_gap_ack")
	require.NoError(t, err)
	f.mu.Lock()
	delete(f.pool, 1)
	f.mu.Unlock()
	tickRecovery(t, s, c, key) // No eligible tickets remain to trigger allocation.
	sent = f.transactions()
	require.Len(t, sent, 4)
	require.Equal(t, sent[1].Hash(), sent[3].Hash())
	require.Equal(t, sent[1].Hash(), sent[2].Hash())
	require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
	var current, phase string
	require.NoError(t, s.db.QueryRow("SELECT tx_hash FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&current))
	require.Equal(t, sent[0].Hash().Hex(), current)
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", sent[1].Hash().Hex()).Scan(&phase))
	require.Equal(t, "superseded", phase)
	f.mine(sent[2], false) // Already-used redemption still fills its nonce slot.
	f.mu.Lock()
	f.finalizedNonce = 2
	f.mu.Unlock()
	next := nextRecoveryTicket(ticket, 233)
	require.NoError(t, s.StoreWinningTicket(next))
	f.mu.Lock()
	f.tickets = append(f.tickets, next)
	f.mu.Unlock()
	tickRecovery(t, s, c, key)
	sent = f.transactions()
	require.EqualValues(t, 2, sent[len(sent)-1].Nonce())
	f.mine(sent[len(sent)-1], true)
	tickRecovery(t, s, c, key)
	require.Equal(t, "confirmed", ticketPhase(t, s, next.Sig))
}

func TestFinalizedExternalNonceRetiresOnlyMissingTransactions(t *testing.T) {
	for _, observation := range []string{"missing", "unfinalized", "receipt-error", "nonce-error", "nonce-null"} {
		t.Run(observation, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 234)
			tickRecovery(t, s, c, key)
			s = reopenRecoveryStore(t, s)
			first := f.transactions()[0]
			require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, "expired"))
			// Eviction alone retains liability; a canonical finalized account
			// observation must prove another transaction consumed this nonce.
			f.mu.Lock()
			delete(f.pool, 0)
			f.mu.Unlock()
			require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
			next := nextRecoveryTicket(ticket, 235)
			next.CreationRound = 5
			require.ErrorIs(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 5, big.NewInt(1000), []*pm.SignedTicket{next}), pm.ErrPayerUnavailable)
			f.mu.Lock()
			f.finalizedNonce = 1
			f.finalizedNonceReads = 0
			f.receiptError = observation == "receipt-error"
			f.finalizedNonceError = observation == "nonce-error"
			f.finalizedNonceNull = observation == "nonce-null"
			if observation == "unfinalized" {
				f.finalized = false
				f.mineLocked(first, false)
			}
			f.mu.Unlock()
			failures := ReconcileSubmitted(t.Context(), s, c)
			require.Equal(t, observation == "receipt-error" || observation == "nonce-error" || observation == "nonce-null", len(failures) > 0)
			items, err := s.SubmittedRedemptions()
			require.NoError(t, err)
			if observation == "missing" {
				require.Empty(t, items)
				require.Equal(t, "expired", ticketPhase(t, s, ticket.Sig))
				require.NoError(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 5, big.NewInt(1000), []*pm.SignedTicket{next}))
				oldEpoch := nextRecoveryTicket(ticket, 236)
				oldEpoch.RecipientRandHash = ticket.RecipientRandHash
				require.ErrorIs(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, ticket.RecipientRandHash, 0, big.NewInt(10000), []*pm.SignedTicket{oldEpoch}), ErrInvalidPayment)
			} else {
				require.Len(t, items, 1, "uncertain or unfinalized receipts retain liability")
			}
			f.mu.Lock()
			reads := f.finalizedNonceReads
			f.mu.Unlock()
			require.LessOrEqual(t, reads, 1, "read finalized account state once per sender")
		})
	}
}

func TestFinalizedExternalNoncePreservesHigherAttempt(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 237)
	f.loseNextResponse = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	tickRecovery(t, s, c, key)
	s = reopenRecoveryStore(t, s)
	f.mu.Lock()
	delete(f.pool, 0)
	f.finalizedNonce = 1
	f.finalizedNonceReads = 0
	f.mu.Unlock()
	require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
	items, err := s.SubmittedRedemptions()
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, f.transactions()[1].Hash(), items[0].Hash)
	require.Equal(t, "submitted", ticketPhase(t, s, ticket.Sig))
	f.mu.Lock()
	reads := f.finalizedNonceReads
	f.mu.Unlock()
	require.Equal(t, 1, reads)
	next := nextRecoveryTicket(ticket, 238)
	next.CreationRound = 5
	require.ErrorIs(t, s.storeWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 5, big.NewInt(1000), []*pm.SignedTicket{next}), pm.ErrPayerUnavailable)
	// Finalized state advances beyond both attempts while the pending endpoint
	// stays stale. New signing must use that observed count, not the saved floor.
	f.mu.Lock()
	delete(f.pool, 1)
	f.finalizedNonce = 3
	f.staleNonce = true
	f.mu.Unlock()
	tickRecovery(t, s, c, key)
	sent := f.transactions()
	require.Len(t, sent, 3)
	require.EqualValues(t, 3, sent[2].Nonce())
	f.mine(sent[2], true)
	tickRecovery(t, s, c, key)
	require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
}

func TestFinalizedNonceAdvancesPastResolvedHistory(t *testing.T) {
	for _, tc := range []struct {
		acknowledged       bool
		pending, finalized uint64
	}{{false, 0, 1}, {true, 0, 3}, {true, 1, 3}} {
		t.Run(fmt.Sprintf("acknowledged=%t/pending=%d", tc.acknowledged, tc.pending), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 239)
			f.loseNextResponse = !tc.acknowledged
			require.Equal(t, !tc.acknowledged, len(ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1))) > 0)
			s = reopenRecoveryStore(t, s)
			if tc.acknowledged {
				f.mine(f.transactions()[0], true)
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
				require.NoError(t, s.StoreWinningTicket(nextRecoveryTicket(ticket, 240)))
			}
			f.mu.Lock()
			delete(f.pool, 0)
			f.finalizedNonce = tc.finalized
			f.staleNonce = true
			f.staleNonceValue = tc.pending
			f.mu.Unlock()
			tickRecovery(t, s, c, key)
			sent := f.transactions()
			require.Len(t, sent, 2)
			require.Equal(t, tc.finalized, sent[1].Nonce(), "local history must not hide newer finalized account state")
		})
	}
}

func nextRecoveryTicket(ticket *pm.SignedTicket, id byte) *pm.SignedTicket {
	next, body := *ticket, *ticket.Ticket
	next.Ticket = &body
	next.Sig = []byte{id, 2, 3}
	next.RecipientRandHash = crypto.Keccak256Hash([]byte{id})
	return &next
}
