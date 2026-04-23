// Package config provides typed configuration for the router service.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all router configuration.
type Config struct {
	Port     int
	LogLevel string

	// Redis (for route caching)
	RedisURL string

	// Timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		Port:         envInt("ROUTER_PORT", 8083),
		LogLevel:     envStr("LOG_LEVEL", "info"),
		RedisURL:     envStr("REDIS_URL", "redis://localhost:6379/0"),
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
