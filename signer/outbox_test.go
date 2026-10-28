package signer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type writerFunc func(context.Context, ...kafka.Message) error

func (f writerFunc) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	return f(ctx, messages...)
}
func (writerFunc) Close() error { return nil }

func testKafkaProducer(t *testing.T, maxBytes int64) *kafkaProducer {
	t.Helper()
	p, err := openKafkaProducer(t.Context(), &KafkaConfig{Broker: kafkaBrokerURL(t, "kafka://127.0.0.1:1"), Topic: "signing", OutboxDB: filepath.Join(t.TempDir(), "events.sqlite"), OutboxMaxBytes: maxBytes}, "payer")
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func testEvent() signingEvent {
	return signingEvent{ID: uuid.NewV4().String(), Type: "create_signed_ticket", Timestamp: "1800000000000", Data: signedTicketEvent{ComputedFee: "10", NumTickets: 1}}
}

func TestKafkaProducerOutboxBindingResolvesURLs(t *testing.T) {
	config := KafkaConfig{Topic: "signing", OutboxDB: filepath.Join(t.TempDir(), "events.sqlite"), OutboxMaxBytes: 1 << 20}
	// Existing outboxes bind to the resolved host:port, without a URL scheme.
	outbox, err := openEventOutbox(t.Context(), config.OutboxDB, outboxBinding{"payer", "broker:9092", config.Topic}, config.OutboxMaxBytes)
	require.NoError(t, err)
	_, err = outbox.enqueue(t.Context(), testEvent())
	require.NoError(t, err)
	original, err := outbox.pending(t.Context())
	require.NoError(t, err)
	require.NoError(t, outbox.Close())
	for _, broker := range []string{"kafka://broker", "kafka://broker:9092"} {
		config.Broker = kafkaBrokerURL(t, broker)
		producer, err := openKafkaProducer(t.Context(), &config, "payer")
		require.NoError(t, err)
		require.Equal(t, "broker:9092", producer.writer.(*kafka.Writer).Addr.String())
		recovered, err := producer.outbox.pending(t.Context())
		require.NoError(t, err)
		require.Equal(t, original, recovered)
		require.NoError(t, producer.Close())
	}
	config.Broker = kafkaBrokerURL(t, "kafka://broker:19092")
	_, err = openKafkaProducer(t.Context(), &config, "payer")
	require.ErrorContains(t, err, "bound")
}

func TestOutboxDurabilityBindingAndPermissions(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "events.sqlite")
	binding := outboxBinding{Signer: "payer", Broker: "broker:9092", Topic: "signing"}
	o, err := openEventOutbox(ctx, path, binding, 1<<20)
	require.NoError(t, err)
	for _, pragma := range []struct{ name, value string }{{"journal_mode", "wal"}, {"synchronous", "2"}} {
		var actual string
		require.NoError(t, o.db.QueryRow("PRAGMA "+pragma.name).Scan(&actual))
		require.Equal(t, pragma.value, actual)
	}
	event := testEvent()
	_, err = o.enqueue(ctx, event)
	require.NoError(t, err)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(path + suffix)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
		require.NoError(t, os.Chmod(path+suffix, 0644))
		_, err = openEventOutbox(ctx, path, binding, 1<<20)
		require.ErrorContains(t, err, "owner-only")
		require.NoError(t, os.Chmod(path+suffix, 0600))
	}
	pending, err := o.pending(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.NoError(t, o.Close())
	for _, changed := range []outboxBinding{{"other", binding.Broker, binding.Topic}, {binding.Signer, "other:9092", binding.Topic}, {binding.Signer, binding.Broker, "other"}} {
		_, err := openEventOutbox(ctx, path, changed, 1<<20)
		require.ErrorContains(t, err, "bound")
	}
	o, err = openEventOutbox(ctx, path, binding, 1<<20)
	require.NoError(t, err)
	recovered, err := o.pending(ctx)
	require.NoError(t, err)
	require.Equal(t, pending, recovered, "restart must preserve ID and payload bytes")
	require.NoError(t, o.acknowledge(ctx, recovered))
	stats, err := o.stats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.Count)
	require.Zero(t, stats.Bytes)
	require.NoError(t, o.Close())
	changed, err := openEventOutbox(ctx, path, outboxBinding{"new-payer", "new:9092", "new-topic"}, 1<<20)
	require.NoError(t, err)
	require.NoError(t, changed.Close())
}

func TestOutboxMigrationRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.sqlite")
	binding := outboxBinding{"payer", "broker:9092", "signing"}
	outbox, err := openEventOutbox(t.Context(), path, binding, 1<<20)
	require.NoError(t, err)
	defer outbox.Close()
	files, err := fs.Sub(outboxMigrationFiles, "migrations")
	require.NoError(t, err)
	require.NoError(t, migrations.Down(t.Context(), outbox.db, files))
	require.NoError(t, outbox.Close())
	outbox, err = openEventOutbox(t.Context(), path, binding, 1<<20)
	require.NoError(t, err, "startup must reapply the rolled-back catalog")
	require.NoError(t, outbox.Close())
}

func TestOutboxCapacityConcurrentAdmissionAndRecovery(t *testing.T) {
	event := testEvent()
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	p := testKafkaProducer(t, int64(len(raw))*3+1)
	var accepted atomic.Int64
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			err := p.Enqueue(t.Context(), testEvent())
			if err == nil {
				accepted.Add(1)
			} else {
				require.ErrorIs(t, err, errOutboxFull)
			}
		})
	}
	workers.Wait()
	require.Equal(t, int64(3), accepted.Load())
	require.False(t, p.ready(t.Context()), "capacity failure must affect readiness even with a few bytes remaining")
	stats, err := p.outbox.stats(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(3), stats.Count)
	require.Equal(t, int64(len(raw))*3, stats.Bytes)
	pending, err := p.outbox.pending(t.Context())
	require.NoError(t, err)
	require.NoError(t, p.outbox.acknowledge(t.Context(), pending[:1]))
	require.True(t, p.ready(t.Context()))
	require.NoError(t, p.Enqueue(t.Context(), testEvent()))
	require.False(t, p.ready(t.Context()))
	require.Equal(t, uint64(9), p.enqueueFailures.Load())
	metrics := collectKafkaMetrics(t, p)
	for _, want := range []string{"pending_events 3", fmt.Sprintf("pending_bytes %d", len(raw)*3), "oldest_event_age_seconds", "enqueue_errors_total 9", "publish_errors_total 0"} {
		require.Contains(t, metrics, want)
	}
}

func TestOutboxStorageWriteFailureAndRecovery(t *testing.T) {
	p := testKafkaProducer(t, 1<<20)
	_, err := p.outbox.db.Exec(`PRAGMA query_only=ON`)
	require.NoError(t, err)
	require.Error(t, p.Enqueue(t.Context(), testEvent()))
	require.False(t, p.ready(t.Context()))
	_, err = p.outbox.db.Exec(`PRAGMA query_only=OFF`)
	require.NoError(t, err)
	require.True(t, p.ready(t.Context()), "readiness must recover without requiring traffic")
	require.NoError(t, p.Enqueue(t.Context(), testEvent()))
	metrics := collectKafkaMetrics(t, p)
	require.Contains(t, metrics, "storage_errors_total 1")
}

func TestOutboxPartialDeliveryRetainsWholeBatch(t *testing.T) {
	p := testKafkaProducer(t, 1<<20)
	m := observability.New("signer")
	m.Legacy(p.collectMetrics)
	for range 3 {
		require.NoError(t, p.Enqueue(t.Context(), testEvent()))
	}
	original, err := p.outbox.pending(t.Context())
	require.NoError(t, err)
	var calls int
	p.writer = writerFunc(func(_ context.Context, messages ...kafka.Message) error {
		calls++
		require.Len(t, messages, len(original))
		for i, message := range messages {
			require.Equal(t, []byte(original[i].ID), message.Key)
			require.Equal(t, original[i].Payload, message.Value)
		}
		if calls == 1 {
			return kafka.WriteErrors{nil, errors.New("uncertain delivery"), nil}
		}
		return nil
	})
	_, err = p.publish(t.Context())
	require.Error(t, err)
	retained, err := p.outbox.pending(t.Context())
	require.NoError(t, err)
	require.Equal(t, original, retained)
	require.Zero(t, test.MetricSum(t, m.Registry, "signer_kafka_delivered_events_total", nil))
	progress, err := p.publish(t.Context())
	require.NoError(t, err)
	require.True(t, progress)
	stats, err := p.outbox.stats(t.Context())
	require.NoError(t, err)
	require.Zero(t, stats.Count)
	require.Zero(t, stats.Bytes)
	require.Equal(t, 2, calls)
	require.EqualValues(t, 3, test.MetricSum(t, m.Registry, "signer_kafka_delivered_events_total", map[string]string{"component": "signer"}))
	require.Positive(t, test.MetricSum(t, m.Registry, "signer_kafka_last_delivery_timestamp_seconds", map[string]string{"component": "signer"}))
	require.Len(t, test.MetricSamples(t, m.Registry, "signer_kafka_pending_events", map[string]string{}), 1)
	for _, label := range test.MetricSamples(t, m.Registry, "signer_kafka_pending_events", nil)[0].Label {
		require.NotEqual(t, "component", label.GetName())
	}
}

func TestPublisherBatchingAndCancellation(t *testing.T) {
	p := testKafkaProducer(t, 1<<20)
	for range 101 {
		require.NoError(t, p.Enqueue(t.Context(), testEvent()))
	}
	require.Len(t, p.wake, 1, "a full batch must wake delivery")
	var sizes []int
	p.writer = writerFunc(func(_ context.Context, messages ...kafka.Message) error {
		sizes = append(sizes, len(messages))
		return nil
	})
	_, err := p.publish(t.Context())
	require.NoError(t, err)
	_, err = p.publish(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int{100, 1}, sizes)
	require.NoError(t, p.Enqueue(t.Context(), testEvent()))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	p.writer = writerFunc(func(ctx context.Context, _ ...kafka.Message) error { cancel(); <-ctx.Done(); return ctx.Err() })
	started := time.Now()
	p.run(ctx)
	require.Less(t, time.Since(started), time.Second, "publisher shutdown blocked")
	stats, err := p.outbox.stats(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Count)
}

func TestPublisherRetriesWithoutChangingEvent(t *testing.T) {
	p := testKafkaProducer(t, 1<<20)
	require.NoError(t, p.Enqueue(t.Context(), testEvent()))
	original, err := p.outbox.pending(t.Context())
	require.NoError(t, err)
	attempts := make(chan time.Time, 2)
	p.writer = writerFunc(func(_ context.Context, messages ...kafka.Message) error {
		require.Equal(t, original[0].Payload, messages[0].Value)
		attempts <- time.Now()
		if len(attempts) == 1 {
			return errors.New("temporary outage")
		}
		return nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { p.run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { stats, err := p.outbox.stats(t.Context()); return err == nil && stats.Count == 0 }, 4*time.Second, 10*time.Millisecond)
	first, second := <-attempts, <-attempts
	require.GreaterOrEqual(t, second.Sub(first), time.Second)
	require.Equal(t, uint64(1), p.publishFailures.Load())
}

func collectKafkaMetrics(t *testing.T, p *kafkaProducer) string {
	t.Helper()
	m := observability.New("signer")
	m.Legacy(p.collectMetrics)
	response := httptest.NewRecorder()
	m.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	return response.Body.String()
}
