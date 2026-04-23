package kafka

import "time"

// Message represents a single Kafka record.
type Message struct {
	// Topic is the destination (publish) or source (consume) topic name.
	Topic string

	// Key is the optional partitioning key. Messages with the same key
	// are guaranteed to land on the same partition.
	Key []byte

	// Value is the message payload, typically JSON or Protobuf-encoded.
	Value []byte

	// Headers carry optional metadata (trace IDs, schema versions, etc.).
	Headers map[string]string

	// Timestamp is set by the broker on produce; read-only on consume.
	Timestamp time.Time

	// Partition and Offset are populated on consume.
	Partition int32
	Offset    int64
}
