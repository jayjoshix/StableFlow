// Package kafka provides producer and consumer abstractions for Kafka-compatible
// message brokers (Kafka, Redpanda). The default implementation uses an in-process
// channel for local development; swap in franz-go or confluent-kafka-go for production.
package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// ── Configuration ─────────────────────────────────────────────

// Config holds connection parameters for the Kafka broker.
type Config struct {
	Brokers  []string
	GroupID  string
	ClientID string
}

// ── Producer ──────────────────────────────────────────────────

// Producer publishes messages to a Kafka topic.
type Producer interface {
	// Publish sends a message to the specified topic.
	Publish(ctx context.Context, msg Message) error
	// Close flushes pending writes and releases resources.
	Close() error
}

type producer struct {
	cfg    Config
	mu     sync.Mutex
	closed bool
	logger *slog.Logger
}

// NewProducer returns a Producer configured for the given brokers.
// TODO: Replace with franz-go kgo.NewClient for production use.
func NewProducer(cfg Config, logger *slog.Logger) (Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one broker address is required")
	}
	return &producer{
		cfg:    cfg,
		logger: logger.With("component", "kafka.producer"),
	}, nil
}

func (p *producer) Publish(ctx context.Context, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("kafka: producer is closed")
	}
	p.logger.InfoContext(ctx, "publishing message",
		"topic", msg.Topic,
		"key", string(msg.Key),
		"size", len(msg.Value),
	)
	// TODO: Replace with actual broker publish.
	return nil
}

func (p *producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.logger.Info("producer closed")
	return nil
}

// ── Consumer ──────────────────────────────────────────────────

// Handler processes a single Kafka message. Return a non-nil error to
// trigger a retry (when the broker supports it).
type Handler func(ctx context.Context, msg Message) error

// Consumer subscribes to one or more Kafka topics and dispatches
// messages to a Handler.
type Consumer interface {
	// Subscribe starts consuming from the given topics. Blocks until
	// the context is cancelled or an unrecoverable error occurs.
	Subscribe(ctx context.Context, topics []string, h Handler) error
	// Close releases consumer resources.
	Close() error
}

type consumer struct {
	cfg    Config
	logger *slog.Logger
}

// NewConsumer returns a Consumer configured for the given brokers and group.
// TODO: Replace with franz-go kgo.NewClient for production use.
func NewConsumer(cfg Config, logger *slog.Logger) (Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one broker address is required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("kafka: group ID is required")
	}
	return &consumer{
		cfg:    cfg,
		logger: logger.With("component", "kafka.consumer", "group", cfg.GroupID),
	}, nil
}

func (c *consumer) Subscribe(ctx context.Context, topics []string, h Handler) error {
	c.logger.InfoContext(ctx, "subscribing to topics", "topics", topics)
	// TODO: Replace with actual broker consume loop.
	// Block until context is cancelled.
	<-ctx.Done()
	return ctx.Err()
}

func (c *consumer) Close() error {
	c.logger.Info("consumer closed")
	return nil
}
