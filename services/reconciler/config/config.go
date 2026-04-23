// Package config provides typed configuration for the reconciler service.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all reconciler configuration.
type Config struct {
	LogLevel string

	// Database
	DatabaseURL string

	// Kafka
	KafkaBrokers []string
	KafkaGroupID string

	// Reconciliation
	Interval     time.Duration // how often to run reconciliation
	BatchSize    int           // max records per reconciliation pass
	MaxRetries   int           // retry count for failed reconciliations
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		LogLevel:     envStr("LOG_LEVEL", "info"),
		DatabaseURL:  envStr("DATABASE_URL", "postgres://stableflow:stableflow@localhost:5432/stableflow?sslmode=disable"),
		KafkaBrokers: envList("KAFKA_BROKERS", []string{"localhost:19092"}),
		KafkaGroupID: envStr("RECONCILER_KAFKA_GROUP", "reconciler-service"),
		Interval:     envDuration("RECONCILER_INTERVAL", 1*time.Minute),
		BatchSize:    envInt("RECONCILER_BATCH_SIZE", 100),
		MaxRetries:   envInt("RECONCILER_MAX_RETRIES", 3),
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
