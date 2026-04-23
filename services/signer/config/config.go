// Package config provides typed configuration for the signer service.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all signer configuration.
type Config struct {
	Port     int
	LogLevel string

	// KMS / HSM
	KMSProvider string // "local", "aws-kms", "gcp-kms", "hashicorp-vault"
	KMSKeyID    string
	KMSEndpoint string

	// Kafka
	KafkaBrokers []string
	KafkaGroupID string

	// Timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		Port:         envInt("SIGNER_PORT", 8084),
		LogLevel:     envStr("LOG_LEVEL", "info"),
		KMSProvider:  envStr("KMS_PROVIDER", "local"),
		KMSKeyID:     envStr("KMS_KEY_ID", ""),
		KMSEndpoint:  envStr("KMS_ENDPOINT", ""),
		KafkaBrokers: envList("KAFKA_BROKERS", []string{"localhost:19092"}),
		KafkaGroupID: envStr("SIGNER_KAFKA_GROUP", "signer-service"),
		ReadTimeout:  envDuration("READ_TIMEOUT", 15*time.Second),
		WriteTimeout: envDuration("WRITE_TIMEOUT", 15*time.Second),
	}
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envList(key string, fallback []string) []string {
	if v := os.Getenv(key); v != "" {
		return strings.Split(v, ",")
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
