// Package config provides typed configuration for the listener service.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all listener configuration.
type Config struct {
	LogLevel string

	// Kafka
	KafkaBrokers []string
	KafkaGroupID string

	// Blockchain RPCs
	EthRPCURL  string
	SolRPCURL  string
	TronRPCURL string

	// Polling
	PollInterval time.Duration
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		LogLevel:     envStr("LOG_LEVEL", "info"),
		KafkaBrokers: envList("KAFKA_BROKERS", []string{"localhost:19092"}),
		KafkaGroupID: envStr("LISTENER_KAFKA_GROUP", "listener-service"),
		EthRPCURL:    envStr("ETH_RPC_URL", "https://rpc.ankr.com/eth"),
		SolRPCURL:    envStr("SOL_RPC_URL", "https://api.mainnet-beta.solana.com"),
		TronRPCURL:   envStr("TRON_RPC_URL", "https://api.trongrid.io"),
		PollInterval: envDuration("LISTENER_POLL_INTERVAL", 5*time.Second),
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
