package orchestrator

import (
	"context"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

type receiptFixture struct{ confirmed, reverted bool }

func (r receiptFixture) Receipt(context.Context, ethcommon.Hash) (bool, bool, error) {
	return r.confirmed, r.reverted, nil
}

func TestRedeemerSQLiteFilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payment.sqlite")
	store, err := OpenRedeemerDB(path)
	require.NoError(t, err)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(path + suffix)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	}
	require.NoError(t, store.Close())
	require.NoError(t, os.Chmod(path, 0644))
	_, err = OpenRedeemerDB(path)
	require.ErrorContains(t, err, "owner-only")
}

func TestRedeemerMigrationRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "redeemer.sqlite")
	store, err := OpenRedeemerDB(path)
	require.NoError(t, err)
	defer store.Close()
	files, err := fs.Sub(redeemerMigrationFiles, "migrations")
	require.NoError(t, err)
	require.NoError(t, migrations.Down(t.Context(), store.db, files))
	require.NoError(t, store.Close())
	store, err = OpenRedeemerDB(path)
	require.NoError(t, err, "startup must reapply the rolled-back catalog")
	require.NoError(t, store.Close())
}

// Adapted from go-livepeer/common/db_test.go's winning-ticket store tests,
// by Elad Mallel and Nico Vergauwen (404d24a9455cd0997af1be6d18faa998702cce69).
func TestSQLiteWinningTicketLifecycle(t *testing.T) {
	store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "recipient.sqlite"))
	require.NoError(t, err)
	defer store.Close()
	payer := ethcommon.HexToAddress("0x1234")
	ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: payer, Recipient: ethcommon.HexToAddress("0x5678"),
		FaceValue: big.NewInt(1234), WinProb: big.NewInt(2345), TicketNonce: 123,
		RecipientRandHash: ethcommon.HexToHash("0xabcd"), CreationRound: 9,
		CreationRoundBlockHash: ethcommon.HexToHash("0x9876"), ParamsExpirationBlock: big.NewInt(1337)},
		Sig: []byte{1, 2, 3}, RecipientRand: big.NewInt(4567)}
	count, err := store.WinningTicketCount(payer, 9)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, store.StoreWinningTicket(ticket))
	require.Error(t, store.StoreWinningTicket(ticket))
	selected, err := store.SelectEarliestWinningTicket(payer, 9)
	require.NoError(t, err)
	require.Equal(t, ticket, selected)
	selected, err = store.SelectEarliestWinningTicket(payer, 10)
	require.NoError(t, err)
	require.Nil(t, selected)
	require.NoError(t, store.RemoveWinningTicket(ticket))
	count, err = store.WinningTicketCount(payer, 9)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestRedemptionReceiptSettlesAttempt(t *testing.T) {
	for _, phase := range []string{"confirmed", "reverted"} {
		t.Run(phase, func(t *testing.T) {
			store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "recipient.sqlite"))
			require.NoError(t, err)
			defer store.Close()
			payer := ethcommon.HexToAddress("0x1234")
			ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: payer, Recipient: ethcommon.HexToAddress("0x5678"), FaceValue: big.NewInt(1), WinProb: big.NewInt(1), TicketNonce: 1, RecipientRandHash: ethcommon.HexToHash("0xabcd"), CreationRound: 1, ParamsExpirationBlock: big.NewInt(10)}, Sig: []byte{1, 2, 3}, RecipientRand: big.NewInt(4)}
			require.NoError(t, store.StoreWinningTicket(ticket))
			hash := ethcommon.HexToHash("0x1234")
			require.NoError(t, store.recordPrepared(t.Context(), ticket, eth.SignedTransaction{Hash: hash, Raw: []byte{1}, From: ticket.Recipient, Nonce: 1}))
			submitted, err := store.SubmittedRedemptions()
			require.NoError(t, err)
			require.Equal(t, []SubmittedRedemption{{Signature: ticket.Sig, Hash: hash}}, submitted)
			failures := ReconcileSubmitted(t.Context(), store, receiptFixture{confirmed: phase == "confirmed", reverted: phase == "reverted"})
			if phase == "reverted" {
				require.Len(t, failures, 1)
				require.ErrorContains(t, failures[0], "reverted")
			} else {
				require.Empty(t, failures)
			}
			attempts, err := store.Redemptions()
			require.NoError(t, err)
			require.Len(t, attempts, 1)
			require.Equal(t, phase, attempts[0].Phase)
			submitted, err = store.SubmittedRedemptions()
			require.NoError(t, err)
			require.Empty(t, submitted)
			pending, err := store.PendingPayers()
			require.NoError(t, err)
			require.Empty(t, pending)
		})
	}
}

func TestRedemptionMigrationHistoryAndRollback(t *testing.T) {
	_, s, ticket, c, key := newRecoveryTest(t, 9)
	files, err := fs.Sub(redeemerMigrationFiles, "migrations")
	require.NoError(t, err)
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	// Seed a genuine v1 database, then upgrade without changing its ticket identity.
	prepared, err := c.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
	require.NoError(t, err)
	_, err = s.db.Exec(`INSERT INTO redemption_attempts(sig,attempted_at,phase,raw_transaction,redeemer_address,nonce) VALUES(?,?,'submitted',?,?,?)`, ticket.Sig, time.Now().UTC().Format(time.RFC3339Nano), prepared.Raw, prepared.From.Hex(), "0")
	require.NoError(t, err)
	_, err = s.db.Exec("UPDATE winning_tickets SET tx_hash=? WHERE sig=?", prepared.Hash.Hex(), ticket.Sig)
	require.NoError(t, err)
	require.NoError(t, migrations.Up(t.Context(), s.db, files))
	history, err := s.Redemptions()
	require.NoError(t, err)
	require.Equal(t, []RedemptionStatus{{Hash: prepared.Hash.Hex(), Phase: "submitted"}}, history)
	// A lossless rollback remains possible with the original representable state.
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Up(t.Context(), s.db, files))
	_, err = s.db.Exec("UPDATE redemption_attempts SET phase='retryable' WHERE sig=?", ticket.Sig)
	require.NoError(t, err)
	next := prepared
	next.Hash = ethcommon.HexToHash("0xabcdef")
	next.Nonce = 1
	require.NoError(t, s.recordPrepared(t.Context(), ticket, next))
	require.ErrorContains(t, migrations.Down(t.Context(), s.db, files), "cannot_downgrade_redemption_transaction_history")
	history, err = s.Redemptions()
	require.NoError(t, err)
	require.Len(t, history, 2)
}

func TestRedemptionRollbackPreservesTransactionProgress(t *testing.T) {
	for _, phase := range []string{"submitted", "expired"} {
		t.Run(phase, func(t *testing.T) {
			_, s, ticket, c, key := newRecoveryTest(t, 22)
			tickRecovery(t, s, c, key)
			s = reopenRecoveryStore(t, s)
			if phase == "expired" {
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, phase))
			}
			floor, err := s.SavedNonceFloor(t.Context(), key.Address())
			require.NoError(t, err)
			require.EqualValues(t, 1, floor)
			files, err := fs.Sub(redeemerMigrationFiles, "migrations")
			require.NoError(t, err)
			require.ErrorContains(t, migrations.Down(t.Context(), s.db, files), "cannot_downgrade_redemption_transaction_history")
			floor, err = s.SavedNonceFloor(t.Context(), key.Address())
			require.NoError(t, err)
			require.EqualValues(t, 1, floor, "protected rollback must leave history intact")
		})
	}
}
