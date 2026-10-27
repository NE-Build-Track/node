package orchestrator

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth"
	"github.com/stretchr/testify/require"
)

func TestRedemptionEligibility(t *testing.T) {
	for _, tc := range []struct {
		block, round        int64
		active, lookupError bool
	}{{9, 5, true, false}, {10, 5, true, false}, {11, 5, true, false}, {10, 5, false, false}, {10, 5, true, true}, {10, 8, true, false}} {
		t.Run(fmt.Sprintf("block=%d/round=%d/active=%t/error=%t", tc.block, tc.round, tc.active, tc.lookupError), func(t *testing.T) {
			f, store, ticket, chain, key := newRecoveryTest(t, 1)
			f.block, f.round, f.inactive, f.activeError = tc.block, tc.round, !tc.active, tc.lookupError
			_, err := store.db.Exec("UPDATE winning_tickets SET creation_round=5")
			require.NoError(t, err)
			f.onSend = func(tx *types.Transaction) {
				items, err := store.Redemptions()
				require.NoError(t, err)
				require.Equal(t, []RedemptionStatus{{Hash: tx.Hash().Hex(), Phase: "broadcast"}}, items, "persist the signed identity before sending")
			}
			failures := ProcessRedemptions(t.Context(), store, chain, key, big.NewInt(1))
			require.Equal(t, tc.lookupError, len(failures) != 0, "%v", failures)
			expired := tc.round > 7
			redeemed := tc.block >= 10 && !expired && tc.active && !tc.lookupError
			require.Equal(t, redeemed, len(f.transactions()) == 1)
			if tc.block >= 10 && !expired {
				f.mu.Lock()
				recipient := f.activeRecipient
				f.mu.Unlock()
				require.Equal(t, ticket.Recipient, recipient, "activity lookup must use the ticket recipient")
			}
			queued, err := store.PendingPayers()
			require.NoError(t, err)
			require.Equal(t, !redeemed && !expired, len(queued) == 1)
		})
	}
}

func TestTerminalRedemptionSubmissionError(t *testing.T) {
	f, store, ticket, chain, key := newRecoveryTest(t, 2)
	f.prepareError = "private endpoint and calldata: " + eth.ErrTicketCreationRound.Error()
	failures := ProcessRedemptions(t.Context(), store, chain, key, big.NewInt(1))
	require.Len(t, failures, 1)
	require.ErrorIs(t, failures[0], eth.ErrTicketCreationRound)
	require.NotContains(t, failures[0].Error(), "private")
	require.Equal(t, "invalid", ticketPhase(t, store, ticket.Sig))
	queued, err := store.PendingPayers()
	require.NoError(t, err)
	require.Empty(t, queued)
	require.Empty(t, f.transactions())
	store = reopenRecoveryStore(t, store)
	f.mu.Lock()
	f.prepareError = ""
	f.mu.Unlock()
	tickRecovery(t, store, chain, key)
	require.Equal(t, "invalid", ticketPhase(t, store, ticket.Sig))
	require.Empty(t, f.transactions(), "a terminal preparation failure must not be submitted after restart")
}

// Upstream queue/sendermonitor tests: terminal classification and retryable
// submission/CheckTx failures retain their assertions against the node adapter.
func TestUpstreamTicketRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		err      error
		terminal bool
	}{{nil, false}, {eth.ErrTicketCreationRound, true}, {fmt.Errorf("wrapped: %w", eth.ErrTicketCreationRound), true}, {fmt.Errorf("transaction failed txHash=abc"), false}, {fmt.Errorf("nonce too low"), false}, {fmt.Errorf("replacement transaction underpriced"), false}, {context.DeadlineExceeded, false}, {fmt.Errorf("checktx error"), false}} {
		require.Equal(t, tc.terminal, isNonRetryableTicketErr(tc.err))
	}
}
