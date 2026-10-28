package signer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/livepeer/node/internal/observability"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const kafkaBatchBytes = 1 << 20

// KafkaBroker supplies the default scheme before URL parsing, which would
// otherwise interpret host:port as scheme:opaque or reject bare IP addresses.
type KafkaBroker url.URL

func (b *KafkaBroker) UnmarshalText(data []byte) error {
	address := string(data)
	if !strings.Contains(address, "://") {
		address = "kafkas://" + strings.TrimPrefix(address, "//")
	}
	parsed, err := url.Parse(address)
	if err != nil {
		return err
	}
	*b = KafkaBroker(*parsed)
	return nil
}

func (b KafkaBroker) MarshalText() ([]byte, error) {
	return []byte(b.String()), nil
}

func (b KafkaBroker) String() string {
	return (*url.URL)(&b).String()
}

// KafkaConfig is a named Boa parameter group: [Kafka] in TOML, --kafka-*
// flags, and LIVEPEER_SIGNER_KAFKA_* environment variables.
type KafkaConfig struct {
	Broker         KafkaBroker `name:"broker" required:"true" descr:"One Kafka bootstrap address (TLS by default; kafka:// selects plaintext)"`
	Topic          string      `name:"topic" required:"true" descr:"Kafka topic for signing events"`
	AuthMethod     string      `name:"auth-method" default:"scram-sha-512" alts:"plain,scram-sha-256,scram-sha-512" strict:"true" descr:"SASL authentication method when credentials are supplied"`
	Username       string      `name:"username" optional:"true" secret:"true"`
	Password       string      `name:"password" optional:"true" secret:"true"`
	UsernameFile   string      `basepath:"source" name:"username-file" secretfor:"Username"`
	PasswordFile   string      `basepath:"source" name:"password-file" secretfor:"Password"`
	OutboxDB       string      `name:"outbox-db" file:"optional" default:"events.sqlite" descr:"Durable signer event SQLite path"`
	OutboxMaxBytes int64       `name:"outbox-max-bytes" default:"268435456" min:"1" descr:"Maximum pending event payload bytes"`
}

func (c KafkaConfig) Validate() error {
	if _, err := c.brokerAddress(); err != nil {
		return err
	}
	if c.Topic == "" || len(c.Topic) > 249 || c.Topic == "." || c.Topic == ".." || strings.ContainsFunc(c.Topic, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-')
	}) {
		return errors.New("--kafka-topic must be 1–249 ASCII letters, digits, '.', '_' or '-', and cannot be '.' or '..'")
	}
	if c.OutboxDB == "" || c.OutboxDB == ":memory:" || strings.HasPrefix(c.OutboxDB, "file:") || strings.Contains(c.OutboxDB, "?") {
		return errors.New("--kafka-outbox-db must be a filesystem path, not :memory:, a file: URI or a path containing '?'")
	}
	if c.OutboxMaxBytes <= 0 {
		return errors.New("--kafka-outbox-max-bytes must be positive")
	}
	if (c.Username == "") != (c.Password == "") || (c.UsernameFile != "" || c.PasswordFile != "") && (c.Username == "" || c.Password == "") {
		return errors.New("kafka username and password must be supplied together: set LIVEPEER_SIGNER_KAFKA_USERNAME and LIVEPEER_SIGNER_KAFKA_PASSWORD, or use --kafka-username-file and --kafka-password-file")
	}
	if strings.ContainsRune(c.Username, 0) || strings.ContainsRune(c.Password, 0) {
		return errors.New("kafka credentials must not contain NUL bytes; check LIVEPEER_SIGNER_KAFKA_USERNAME or --kafka-username-file and LIVEPEER_SIGNER_KAFKA_PASSWORD or --kafka-password-file")
	}
	_, err := c.transport()
	return err
}

func (c KafkaConfig) brokerAddress() (string, error) {
	broker := (*url.URL)(&c.Broker)
	if broker.Host == "" {
		return "", errors.New("kafka broker is required when Kafka is enabled: set --kafka-broker or LIVEPEER_SIGNER_KAFKA_BROKER")
	}
	if (broker.Scheme != "kafka" && broker.Scheme != "kafkas") ||
		broker.User != nil || broker.Path != "" || broker.RawPath != "" || broker.Opaque != "" ||
		broker.RawQuery != "" || broker.ForceQuery || broker.Fragment != "" || broker.RawFragment != "" {
		return "", errors.New("--kafka-broker must be one host[:port] address with optional kafka:// or kafkas:// scheme and no credentials, path, query or fragment")
	}
	address := broker.Host
	if broker.Port() == "" && !strings.HasSuffix(address, ":") {
		// These are signer conventions, not protocol-defined Kafka ports.
		// An explicit URL port always wins, including provider-specific ports.
		var port string
		switch {
		case c.Username != "" && c.AuthMethod == "plain":
			port = "9094"
		case c.Username != "":
			port = "9096"
		case broker.Scheme == "kafka":
			port = "9092"
		default:
			port = "9093"
		}
		address += ":" + port
	}
	// url.Parse validates bracketed IPv6, but non-HTTP schemes still allow
	// unbracketed colons. SplitHostPort rejects those ambiguous authorities.
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || strings.Contains(host, ",") {
		return "", errors.New("--kafka-broker must have one host and an optional numeric port; IPv6 addresses must be bracketed")
	}
	// URL ports are numeric strings, without a TCP/UDP port range constraint.
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return "", errors.New("--kafka-broker port must be between 1 and 65535")
	}
	return net.JoinHostPort(host, strconv.FormatUint(portNumber, 10)), nil
}

type eventWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type kafkaProducer struct {
	outbox          *eventOutbox
	writer          eventWriter
	transport       *kafka.Transport
	wake            chan struct{}
	enqueueFailures atomic.Uint64
	publishFailures atomic.Uint64
	storageFailures atomic.Uint64
	deliveredEvents atomic.Uint64
	lastDelivery    atomic.Int64
}

func (c KafkaConfig) transport() (*kafka.Transport, error) {
	transport := &kafka.Transport{DialTimeout: 2 * time.Second}
	if c.Broker.Scheme == "kafkas" {
		transport.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	switch c.AuthMethod {
	case "plain":
		if c.Username != "" {
			transport.SASL = plain.Mechanism{Username: c.Username, Password: c.Password}
		}
	case "", "scram-sha-512", "scram-sha-256":
		if c.Username != "" {
			algorithm := scram.SHA512
			if c.AuthMethod == "scram-sha-256" {
				algorithm = scram.SHA256
			}
			var err error
			transport.SASL, err = scram.Mechanism(algorithm, c.Username, c.Password)
			if err != nil {
				return nil, errors.New("invalid Kafka SCRAM credentials; check LIVEPEER_SIGNER_KAFKA_USERNAME or --kafka-username-file and LIVEPEER_SIGNER_KAFKA_PASSWORD or --kafka-password-file")
			}
		}
	default:
		return nil, errors.New("--kafka-auth-method must be plain, scram-sha-256 or scram-sha-512")
	}
	return transport, nil
}

func openKafkaProducer(ctx context.Context, c *KafkaConfig, signer string) (*kafkaProducer, error) {
	if c == nil {
		return nil, nil
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	address, err := c.brokerAddress()
	if err != nil {
		return nil, err
	}
	transport, err := c.transport()
	if err != nil {
		return nil, err
	}
	outbox, err := openEventOutbox(ctx, c.OutboxDB, outboxBinding{Signer: signer, Broker: address, Topic: c.Topic}, c.OutboxMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("open Kafka outbox %q (--kafka-outbox-db or LIVEPEER_SIGNER_KAFKA_OUTBOX_DB): %w", c.OutboxDB, err)
	}
	writer := &kafka.Writer{
		Addr: kafka.TCP(address), Topic: c.Topic, Balancer: kafka.CRC32Balancer{}, Transport: transport,
		RequiredAcks: kafka.RequireAll, MaxAttempts: 1, BatchSize: 100, BatchBytes: kafkaBatchBytes, BatchTimeout: 10 * time.Millisecond,
		ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
	}
	return &kafkaProducer{outbox: outbox, writer: writer, transport: transport, wake: make(chan struct{}, 1)}, nil
}

func (p *kafkaProducer) Enqueue(ctx context.Context, event signingEvent) error {
	count, err := p.outbox.enqueue(ctx, event)
	if err != nil {
		p.enqueueFailures.Add(1)
		if !errors.Is(err, errOutboxFull) && !errors.Is(err, errEventTooLarge) {
			p.storageFailures.Add(1)
		}
		return err
	}
	if count >= 100 {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Any partial or uncertain write retains the whole batch. The consumer must
// deduplicate replay using the event ID and exact serialized payload.
func (p *kafkaProducer) publish(ctx context.Context) (bool, error) {
	events, err := p.outbox.pending(ctx)
	if err != nil {
		p.storageFailures.Add(1)
		return false, err
	}
	if len(events) == 0 {
		return false, nil
	}
	messages := make([]kafka.Message, len(events))
	for i, event := range events {
		messages[i] = kafka.Message{Key: []byte(event.ID), Value: event.Payload}
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = p.writer.WriteMessages(writeCtx, messages...)
	cancel()
	if err != nil {
		p.publishFailures.Add(1)
		return false, err
	}
	if err := p.outbox.acknowledge(ctx, events); err != nil {
		p.storageFailures.Add(1)
		return false, err
	}
	p.deliveredEvents.Add(uint64(len(events)))
	p.lastDelivery.Store(time.Now().Unix())
	return true, nil
}

func (p *kafkaProducer) run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		progress, err := p.publish(ctx)
		if err == nil && progress {
			backoff = time.Second
			continue
		}
		delay := time.Second
		var wake <-chan struct{} = p.wake
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.With("component", "signer", "event", "accounting_delivery", "operation", "accounting_delivery").WarnContext(ctx, "signer Kafka delivery failed; accounting events retained", "retry_after", backoff, "outcome", "error", "error_kind", observability.ErrorKind(err))
			delay, wake = backoff, nil
			backoff = min(backoff*2, 30*time.Second)
		}
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		case <-wake:
		}
	}
}

func (p *kafkaProducer) ready(ctx context.Context) bool {
	return p == nil || p.outbox.ready(ctx)
}

func (p *kafkaProducer) collectMetrics(emit observability.Emit) {
	if p == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stats, err := p.outbox.stats(ctx)
	emit("signer_kafka_outbox_healthy", observability.Bool(err == nil && !p.outbox.unhealthy.Load()), nil)
	if err == nil {
		age := 0.0
		if !stats.Oldest.IsZero() {
			age = max(time.Since(stats.Oldest).Seconds(), 0)
		}
		emit("signer_kafka_pending_events", float64(stats.Count), nil)
		emit("signer_kafka_pending_bytes", float64(stats.Bytes), nil)
		emit("signer_kafka_oldest_event_age_seconds", age, nil)
	}
	emit("signer_kafka_publish_errors_total", float64(p.publishFailures.Load()), nil)
	emit("signer_kafka_enqueue_errors_total", float64(p.enqueueFailures.Load()), nil)
	emit("signer_kafka_storage_errors_total", float64(p.storageFailures.Load()), nil)
	emit("signer_kafka_delivered_events_total", float64(p.deliveredEvents.Load()), observability.Labels{"component": "signer"})
	emit("signer_kafka_last_delivery_timestamp_seconds", float64(p.lastDelivery.Load()), observability.Labels{"component": "signer"})
}

// Call after run has exited; network operations are bounded by the writer's
// read, write and dial timeouts even if a WriteMessages context was canceled.
func (p *kafkaProducer) Close() error {
	if p == nil {
		return nil
	}
	err := p.writer.Close()
	if p.transport != nil {
		p.transport.CloseIdleConnections()
	}
	return errors.Join(err, p.outbox.Close())
}
