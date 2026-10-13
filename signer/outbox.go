package signer

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/livepeer/node/nodeconfig/migrations"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var outboxMigrationFiles embed.FS

var (
	errOutboxFull    = errors.New("signer Kafka outbox capacity exhausted")
	errEventTooLarge = errors.New("signing event exceeds the outbox or Kafka record limit")
)

type outboxBinding struct {
	Signer, Broker, Topic string
}

type eventOutbox struct {
	db             *sql.DB
	binding        outboxBinding
	maxBytes       int64
	unhealthy      atomic.Bool
	capacityNeeded atomic.Int64
}

type pendingEvent struct {
	Seq     int64
	ID      string
	Payload []byte
}

type outboxStats struct {
	Count, Bytes int64
	Oldest       time.Time
}

func openEventOutbox(ctx context.Context, path string, binding outboxBinding, maxBytes int64) (_ *eventOutbox, err error) {
	if maxBytes <= 0 {
		return nil, errors.New("kafka outbox requires a filesystem path and positive capacity")
	}
	db, err := openOutboxDB(ctx, path, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	files, err := fs.Sub(outboxMigrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	if err := migrations.Up(ctx, db, files); err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO signer_kafka_state(id,signer,broker,topic) VALUES(1,?,?,?)
		ON CONFLICT(id) DO UPDATE SET signer=excluded.signer,broker=excluded.broker,topic=excluded.topic WHERE pending_count=0`, binding.Signer, binding.Broker, binding.Topic); err != nil {
		return nil, err
	}
	o := &eventOutbox{db: db, binding: binding, maxBytes: maxBytes}
	if _, err := o.stats(ctx); err != nil {
		return nil, err
	}
	return o, nil
}

func openOutboxDB(ctx context.Context, path string, create bool) (_ *sql.DB, err error) {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") || strings.Contains(path, "?") {
		return nil, errors.New("kafka outbox requires a filesystem path")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, statErr := os.Lstat(path + suffix); statErr == nil && (!info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0) {
			return nil, errors.New("kafka outbox and sidecars must be owner-only regular files")
		}
	}
	flags := os.O_RDWR
	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, errors.New("kafka outbox file is unavailable")
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	db.SetMaxOpenConns(1)
	for _, stmt := range []string{
		"PRAGMA busy_timeout=1000", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (o *eventOutbox) Close() error { return o.db.Close() }

func (o *eventOutbox) enqueue(ctx context.Context, event signingEvent) (count int64, err error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	if event.ID == "" || event.Type != "create_signed_ticket" {
		return 0, errors.New("invalid signing event")
	}
	// Reserve 1 KiB for the UUID key and Kafka record/batch framing.
	if int64(len(payload)) > min(o.maxBytes, kafkaBatchBytes-1024) {
		return 0, errEventTooLarge
	}
	defer func() { o.recordStoreResult(err) }()
	result, err := o.db.ExecContext(ctx, `INSERT INTO signer_kafka_events(event_id,payload,created_ms)
		SELECT ?,?,? FROM signer_kafka_state WHERE id=1 AND signer=? AND broker=? AND topic=? AND pending_bytes<=?`,
		event.ID, payload, time.Now().UnixMilli(), o.binding.Signer, o.binding.Broker, o.binding.Topic, o.maxBytes-int64(len(payload)))
	if err != nil {
		return 0, err
	}
	stats, err := o.stats(ctx)
	if err != nil {
		return 0, err
	}
	if inserted, _ := result.RowsAffected(); inserted == 0 {
		// Not ready until enough space is available.
		o.capacityNeeded.Store(int64(len(payload)))
		return 0, errOutboxFull
	}
	return stats.Count, nil
}

func (o *eventOutbox) recordStoreResult(err error) {
	if !errors.Is(err, errOutboxFull) {
		o.unhealthy.Store(err != nil)
	}
}

func (o *eventOutbox) stats(ctx context.Context) (stats outboxStats, err error) {
	var binding outboxBinding
	var oldest sql.NullInt64
	err = o.db.QueryRowContext(ctx, `SELECT signer,broker,topic,pending_count,pending_bytes,
		(SELECT created_ms FROM signer_kafka_events ORDER BY seq LIMIT 1) FROM signer_kafka_state WHERE id=1`).Scan(
		&binding.Signer, &binding.Broker, &binding.Topic, &stats.Count, &stats.Bytes, &oldest)
	if err == nil && binding != o.binding {
		err = errors.New("kafka outbox is bound to another signer, broker or topic")
	}
	if oldest.Valid {
		stats.Oldest = time.UnixMilli(oldest.Int64)
	}
	return stats, err
}

func (o *eventOutbox) ready(ctx context.Context) bool {
	// A failed write can be transient (disk full, lock, cancellation). Probe a
	// real write before marking storage healthy again, even with an empty queue.
	if o.unhealthy.Load() {
		_, err := o.db.ExecContext(ctx, `UPDATE signer_kafka_state SET probe=1-probe WHERE id=1`)
		if err != nil {
			return false
		}
		o.unhealthy.Store(false)
	}
	stats, err := o.stats(ctx)
	return err == nil && stats.Bytes < o.maxBytes && o.capacityNeeded.Load() <= o.maxBytes-stats.Bytes
}

func (o *eventOutbox) pending(ctx context.Context) (events []pendingEvent, err error) {
	defer func() {
		if err != nil {
			o.recordStoreResult(err)
		}
	}()
	if _, err := o.stats(ctx); err != nil {
		return nil, err
	}
	// Read the binding and events in one snapshot: even an accidental second
	// process rebinding an empty queue cannot send new rows to our old topic.
	rows, err := o.db.QueryContext(ctx, `SELECT seq,event_id,payload FROM signer_kafka_events
		WHERE EXISTS(SELECT 1 FROM signer_kafka_state WHERE id=1 AND signer=? AND broker=? AND topic=?)
		ORDER BY seq LIMIT 100`, o.binding.Signer, o.binding.Broker, o.binding.Topic)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var event pendingEvent
		if err := rows.Scan(&event.Seq, &event.ID, &event.Payload); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (o *eventOutbox) acknowledge(ctx context.Context, events []pendingEvent) (err error) {
	defer func() { o.recordStoreResult(err) }()
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, event := range events {
		if _, err := tx.ExecContext(ctx, `DELETE FROM signer_kafka_events WHERE seq=? AND event_id=?`, event.Seq, event.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}
