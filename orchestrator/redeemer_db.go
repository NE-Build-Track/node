// Winning-ticket store operations follow livepeer/go-livepeer/common/db.go,
// by Elad Mallel and Nico Vergauwen (404d24a9455cd0997af1be6d18faa998702cce69).
// The redemption recovery schema and transaction state are implemented here.
package orchestrator

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/livepeer/node/pm"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var redeemerMigrationFiles embed.FS

// RedeemerDB owns the orchestrator redeemer's durable winning-ticket queue,
// redemption lifecycle, and chain activity observations.
type RedeemerDB struct {
	logger     *slog.Logger
	metrics    *observability.Metrics
	readiness  *paidReadiness
	db         *sql.DB
	recoveryMu sync.Mutex
	recovery   *redemptionRecovery
}

func OpenRedeemerDB(path string) (*RedeemerDB, error) {
	ctx := context.Background()
	db, err := openRedeemerDB(ctx, path, true)
	if err != nil {
		return nil, err
	}
	files, err := fs.Sub(redeemerMigrationFiles, "migrations")
	if err == nil {
		err = migrations.Up(ctx, db, files)
	}
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize redeemer SQLite: %w", err)
	}
	return &RedeemerDB{db: db}, nil
}

func openRedeemerDB(ctx context.Context, path string, create bool) (*sql.DB, error) {
	if path == "" {
		return nil, errors.New("redeemer SQLite path is required")
	}
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("redeemer SQLite state requires a filesystem path")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	flags := os.O_RDWR
	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, errors.New("redeemer SQLite file is unavailable")
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("redeemer SQLite file must be owner-only")
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, stmt := range []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("initialize redeemer SQLite: %w", err)
		}
	}
	return db, nil
}

func (s *RedeemerDB) Close() error {
	s.recoveryMu.Lock()
	r := s.recovery
	s.recoveryMu.Unlock()
	if r != nil {
		r.close()
	}
	return s.db.Close()
}

func (s *RedeemerDB) StoreWinningTicket(t *pm.SignedTicket) error {
	if err := validStoredTicket(t); err != nil {
		return err
	}
	_, err := s.db.Exec(`INSERT INTO winning_tickets
		(payer_address,recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block,enqueued_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, t.PayerAddress.Hex(), t.Recipient.Hex(), t.FaceValue.Bytes(), t.WinProb.Bytes(),
		t.TicketNonce, t.RecipientRand.Bytes(), t.RecipientRandHash.Hex(), t.Sig, t.CreationRound,
		t.CreationRoundBlockHash.Hex(), t.ParamsExpirationBlock.String(), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// storeWinningTickets commits a validated receipt batch before the payment
// engine publishes replay guards or credit. Check exposure and liability in the
// same transaction so redemption and concurrent receipts cannot race these gates.
func (s *RedeemerDB) storeWinningTickets(ctx context.Context, payer ethcommon.Address, randHash ethcommon.Hash, minRound int64, collateral *big.Int, tickets []*pm.SignedTicket) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Once redemption can reveal randomness, this epoch must never accept
	// more tickets, even if a reorg moves the observed L1 clock backwards.
	var exposed int
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM winning_tickets w JOIN redemption_attempts a ON a.sig=w.sig WHERE w.recipient_rand_hash=? AND (a.phase!='expired' OR EXISTS(SELECT 1 FROM redemption_transactions t WHERE t.sig=w.sig)))`, randHash.Hex()).Scan(&exposed); err != nil {
		return err
	}
	if exposed != 0 {
		return ErrInvalidPayment
	}
	for _, ticket := range tickets {
		_, err := tx.Exec(`INSERT INTO winning_tickets(payer_address,recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block,enqueued_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, payer.Hex(), ticket.Recipient.Hex(), ticket.FaceValue.Bytes(), ticket.WinProb.Bytes(), ticket.TicketNonce, ethcommon.LeftPadBytes(ticket.RecipientRand.Bytes(), 32), ticket.RecipientRandHash.Hex(), ticket.Sig, ticket.CreationRound, ticket.CreationRoundBlockHash.Hex(), ticket.ParamsExpirationBlock.String(), time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
	}
	// Include every unconfirmed winner, across all sessions for this payer.
	// Keep uncertain transactions reserved until finalized receipt reconciliation.
	rows, err := tx.Query(`SELECT face_value FROM winning_tickets WHERE payer_address=? AND redeemed_at IS NULL AND (
 (creation_round>=? AND sig NOT IN (SELECT sig FROM redemption_attempts WHERE phase IN ('used','invalid','expired','reverted')))
 OR EXISTS(SELECT 1 FROM redemption_transactions t WHERE t.sig=winning_tickets.sig AND t.phase NOT IN ('confirmed','reverted','superseded')))`, payer.Hex(), minRound)
	if err != nil {
		return err
	}
	pending := new(big.Int)
	for rows.Next() {
		var face []byte
		if err := rows.Scan(&face); err != nil {
			rows.Close()
			return err
		}
		pending.Add(pending, new(big.Int).SetBytes(face))
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if pending.Cmp(collateral) > 0 {
		return pm.ErrPayerUnavailable
	}
	return tx.Commit()
}

func validStoredTicket(t *pm.SignedTicket) error {
	if t == nil || t.Ticket == nil || t.FaceValue == nil || t.WinProb == nil || t.ParamsExpirationBlock == nil || t.RecipientRand == nil || len(t.Sig) == 0 {
		return errors.New("incomplete signed ticket")
	}
	if t.FaceValue.Sign() <= 0 || t.WinProb.Sign() <= 0 || t.ParamsExpirationBlock.Sign() < 0 {
		return errors.New("invalid signed ticket values")
	}
	return nil
}

func (s *RedeemerDB) SelectEarliestWinningTicket(payer ethcommon.Address, minCreationRound int64) (*pm.SignedTicket, error) {
	row := s.db.QueryRow(`SELECT payer_address,recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block
		FROM winning_tickets WHERE payer_address=? AND creation_round>=? AND redeemed_at IS NULL AND sig NOT IN (SELECT sig FROM redemption_attempts WHERE phase!='retryable') AND NOT EXISTS(SELECT 1 FROM winning_tickets other JOIN redemption_attempts a ON a.sig=other.sig WHERE other.payer_address=winning_tickets.payer_address AND a.phase IN ('prepared','broadcast','submitted','mined','awaiting_finality')) ORDER BY seq LIMIT 1`, payer.Hex(), minCreationRound)
	ticket, err := scanStoredTicket(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return ticket, err
}

func scanStoredTicket(row *sql.Row) (*pm.SignedTicket, error) {
	var payer, recipient, randHash, blockHash, expiration string
	var face, prob, rand, sig []byte
	var nonce int64
	var round int64
	if err := row.Scan(&payer, &recipient, &face, &prob, &nonce, &rand, &randHash, &sig, &round, &blockHash, &expiration); err != nil {
		return nil, err
	}
	exp, ok := new(big.Int).SetString(expiration, 10)
	if !ok || nonce < 0 || nonce > 1<<32-1 {
		return nil, errors.New("corrupt winning ticket")
	}
	return &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: ethcommon.HexToAddress(payer), Recipient: ethcommon.HexToAddress(recipient),
		FaceValue: new(big.Int).SetBytes(face), WinProb: new(big.Int).SetBytes(prob), TicketNonce: uint32(nonce),
		RecipientRandHash: ethcommon.HexToHash(randHash), CreationRound: round,
		CreationRoundBlockHash: ethcommon.HexToHash(blockHash), ParamsExpirationBlock: exp},
		Sig: sig, RecipientRand: new(big.Int).SetBytes(rand)}, nil
}

func (s *RedeemerDB) winningTicket(ctx context.Context, sig []byte) (*pm.SignedTicket, error) {
	row := s.db.QueryRowContext(ctx, `SELECT payer_address,recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block FROM winning_tickets WHERE sig=?`, sig)
	return scanStoredTicket(row)
}

func (s *RedeemerDB) WinningTicketCount(payer ethcommon.Address, minCreationRound int64) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM winning_tickets WHERE payer_address=? AND creation_round>=? AND redeemed_at IS NULL AND sig NOT IN (SELECT sig FROM redemption_attempts WHERE phase!='retryable')`, payer.Hex(), minCreationRound).Scan(&count)
	return count, err
}

type SubmittedRedemption struct {
	Signature []byte
	Hash      ethcommon.Hash
}

func (s *RedeemerDB) SubmittedRedemptions() ([]SubmittedRedemption, error) {
	rows, err := s.db.Query(`SELECT t.sig,t.hash FROM redemption_transactions t JOIN redemption_attempts a ON a.sig=t.sig WHERE a.phase NOT IN ('confirmed','used') AND t.phase NOT IN ('confirmed','reverted','superseded') ORDER BY t.rowid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SubmittedRedemption
	for rows.Next() {
		var item SubmittedRedemption
		var hash string
		if err := rows.Scan(&item.Signature, &hash); err != nil {
			return nil, err
		}
		if !ethcommon.IsHexHash(hash) {
			return nil, errors.New("corrupt redemption transaction hash")
		}
		item.Hash = ethcommon.HexToHash(hash)
		out = append(out, item)
	}
	return out, rows.Err()
}
func (s *RedeemerDB) RemoveWinningTicket(t *pm.SignedTicket) error {
	if t == nil || t.Ticket == nil || len(t.Sig) == 0 {
		return errors.New("incomplete signed ticket")
	}
	_, err := s.db.Exec("DELETE FROM winning_tickets WHERE sig=?", t.Sig)
	return err
}

func (s *RedeemerDB) PendingPayers() ([]ethcommon.Address, error) {
	rows, err := s.db.Query(`SELECT DISTINCT payer_address FROM winning_tickets WHERE redeemed_at IS NULL AND sig NOT IN (SELECT sig FROM redemption_attempts WHERE phase!='retryable')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ethcommon.Address
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if !ethcommon.IsHexAddress(raw) {
			return nil, errors.New("corrupt ticket payer")
		}
		result = append(result, ethcommon.HexToAddress(raw))
	}
	return result, rows.Err()
}

var _ pm.TicketStore = (*RedeemerDB)(nil)
