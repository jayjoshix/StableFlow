// Package config provides typed configuration for the gateway service.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all gateway configuration.
type Config struct {
	Port     int
	LogLevel string

	// Auth
	JWTSecret string
	APIKeys   []string

	// Kafka
	KafkaBrokers []string

	// Downstream services
	LedgerAddr string
	RouterAddr string

	// Timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		Port:         envInt("GATEWAY_PORT", 8080),
		LogLevel:     envStr("LOG_LEVEL", "info"),
		JWTSecret:    envStr("JWT_SECRET", "change-me-in-production"),
		APIKeys:      envList("API_KEYS", []string{"sf_dev_key_01"}),
		KafkaBrokers: envList("KAFKA_BROKERS", []string{"localhost:19092"}),
		LedgerAddr:   envStr("LEDGER_ADDR", "localhost:8081"),
		RouterAddr:   envStr("ROUTER_ADDR", "localhost:8083"),
		ReadTimeout:  envDuration("READ_TIMEOUT", 15*time.Second),
		WriteTimeout: envDuration("WRITE_TIMEOUT", 15*time.Second),
	}
}

// ── Helpers ───────────────────────────────────────────────────

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
