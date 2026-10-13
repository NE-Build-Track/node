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
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/livepeer/node/pm"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var redeemerMigrationFiles embed.FS

// RedeemerDB owns the orchestrator redeemer's durable winning-ticket queue,
// redemption lifecycle, and chain activity observations.
type RedeemerDB struct{ db *sql.DB }

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

func (s *RedeemerDB) Close() error { return s.db.Close() }

func (s *RedeemerDB) StoreWinningTicket(t *pm.SignedTicket) error {
	if err := validStoredTicket(t); err != nil {
		return err
	}
	_, err := s.db.Exec(`INSERT INTO winning_tickets
		(payer_address,recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, t.PayerAddress.Hex(), t.Recipient.Hex(), t.FaceValue.Bytes(), t.WinProb.Bytes(),
		t.TicketNonce, t.RecipientRand.Bytes(), t.RecipientRandHash.Hex(), t.Sig, t.CreationRound,
		t.CreationRoundBlockHash.Hex(), t.ParamsExpirationBlock.String())
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
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM winning_tickets w JOIN redemption_attempts a ON a.sig=w.sig WHERE w.recipient_rand_hash=? AND a.phase!='expired')`, randHash.Hex()).Scan(&exposed); err != nil {
		return err
	}
	if exposed != 0 {
		return ErrInvalidPayment
	}
	for _, ticket := range tickets {
		_, err := tx.Exec(`INSERT INTO winning_tickets(payer_address,recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`, payer.Hex(), ticket.Recipient.Hex(), ticket.FaceValue.Bytes(), ticket.WinProb.Bytes(), ticket.TicketNonce, ethcommon.LeftPadBytes(ticket.RecipientRand.Bytes(), 32), ticket.RecipientRandHash.Hex(), ticket.Sig, ticket.CreationRound, ticket.CreationRoundBlockHash.Hex(), ticket.ParamsExpirationBlock.String())
		if err != nil {
			return err
		}
	}
	// Include every unconfirmed winner, across all sessions for this payer.
	// Keep uncertain transactions reserved until finalized receipt reconciliation.
	rows, err := tx.Query("SELECT face_value FROM winning_tickets WHERE payer_address=? AND redeemed_at IS NULL AND creation_round>=? AND sig NOT IN (SELECT sig FROM redemption_attempts WHERE phase='reverted')", payer.Hex(), minRound)
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
	row := s.db.QueryRow(`SELECT recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block
		FROM winning_tickets WHERE payer_address=? AND creation_round>=? AND tx_hash IS NULL AND sig NOT IN (SELECT sig FROM redemption_attempts) ORDER BY seq LIMIT 1`, payer.Hex(), minCreationRound)
	var recipient, randHash, blockHash, expiration string
	var face, prob, rand, sig []byte
	var nonce int64
	var round int64
	if err := row.Scan(&recipient, &face, &prob, &nonce, &rand, &randHash, &sig, &round, &blockHash, &expiration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	exp, ok := new(big.Int).SetString(expiration, 10)
	if !ok || nonce < 0 || nonce > 1<<32-1 {
		return nil, errors.New("corrupt winning ticket")
	}
	return &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: payer, Recipient: ethcommon.HexToAddress(recipient),
		FaceValue: new(big.Int).SetBytes(face), WinProb: new(big.Int).SetBytes(prob), TicketNonce: uint32(nonce),
		RecipientRandHash: ethcommon.HexToHash(randHash), CreationRound: round,
		CreationRoundBlockHash: ethcommon.HexToHash(blockHash), ParamsExpirationBlock: exp},
		Sig: sig, RecipientRand: new(big.Int).SetBytes(rand)}, nil
}

func (s *RedeemerDB) WinningTicketCount(payer ethcommon.Address, minCreationRound int64) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM winning_tickets WHERE payer_address=? AND creation_round>=? AND tx_hash IS NULL`, payer.Hex(), minCreationRound).Scan(&count)
	return count, err
}

type SubmittedRedemption struct {
	Signature []byte
	Hash      ethcommon.Hash
}

func (s *RedeemerDB) SubmittedRedemptions() ([]SubmittedRedemption, error) {
	rows, err := s.db.Query(`SELECT w.sig,w.tx_hash FROM winning_tickets w JOIN redemption_attempts a ON a.sig=w.sig WHERE w.tx_hash IS NOT NULL AND w.redeemed_at IS NULL AND a.phase NOT IN ('confirmed','reverted','expired')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []SubmittedRedemption
	for rows.Next() {
		var sig []byte
		var raw string
		if err := rows.Scan(&sig, &raw); err != nil {
			return nil, err
		}
		if !ethcommon.IsHexHash(raw) {
			return nil, errors.New("corrupt redemption transaction hash")
		}
		result = append(result, SubmittedRedemption{Signature: sig, Hash: ethcommon.HexToHash(raw)})
	}
	return result, rows.Err()
}

func (s *RedeemerDB) ConfirmRedemption(sig []byte) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`UPDATE winning_tickets SET redeemed_at=? WHERE sig=? AND tx_hash IS NOT NULL AND redeemed_at IS NULL`, time.Now().UTC().Format(time.RFC3339Nano), sig)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("redemption is absent or already confirmed")
	}
	_, err = tx.Exec("UPDATE redemption_attempts SET phase='confirmed',error=NULL WHERE sig=?", sig)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RedeemerDB) FailRedemption(sig []byte) error {
	result, err := s.db.Exec(`UPDATE redemption_attempts SET phase='reverted',error='transaction reverted' WHERE sig=? AND phase!='reverted'`, sig)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("redemption attempt is absent or already failed")
	}
	return nil
}

func (s *RedeemerDB) RemoveWinningTicket(t *pm.SignedTicket) error {
	if t == nil || t.Ticket == nil || len(t.Sig) == 0 {
		return errors.New("incomplete signed ticket")
	}
	_, err := s.db.Exec("DELETE FROM winning_tickets WHERE sig=?", t.Sig)
	return err
}

func (s *RedeemerDB) PendingPayers() ([]ethcommon.Address, error) {
	rows, err := s.db.Query(`SELECT DISTINCT payer_address FROM winning_tickets WHERE tx_hash IS NULL AND sig NOT IN (SELECT sig FROM redemption_attempts)`)
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
