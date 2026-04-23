// Package config provides typed configuration for the ledger service.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all ledger configuration.
type Config struct {
	Port     int
	LogLevel string

	// Database
	DatabaseURL string

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
		Port:         envInt("LEDGER_PORT", 8081),
		LogLevel:     envStr("LOG_LEVEL", "info"),
		DatabaseURL:  envStr("DATABASE_URL", "postgres://stableflow:stableflow@localhost:5432/stableflow?sslmode=disable"),
		KafkaBrokers: envList("KAFKA_BROKERS", []string{"localhost:19092"}),
		KafkaGroupID: envStr("LEDGER_KAFKA_GROUP", "ledger-service"),
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
