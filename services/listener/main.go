// Listener watches blockchain networks for on-chain transaction
// confirmations and publishes finality events to Kafka.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stableflow/stableflow/pkg/finality"
	"github.com/stableflow/stableflow/pkg/kafka"
	"github.com/stableflow/stableflow/services/listener/config"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)
	logger.Info("starting listener service", "poll_interval", cfg.PollInterval)

	producer, err := kafka.NewProducer(kafka.Config{
		Brokers:  cfg.KafkaBrokers,
		ClientID: "listener",
	}, logger)
	if err != nil {
		logger.Error("failed to create kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chains := []finality.Chain{finality.Ethereum, finality.Solana, finality.Tron}
	for _, chain := range chains {
		go watchChain(ctx, logger, producer, chain, cfg.PollInterval)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("listener stopping")
	cancel()
	time.Sleep(2 * time.Second)
	logger.Info("listener stopped")
}

func watchChain(ctx context.Context, logger *slog.Logger, producer kafka.Producer, chain finality.Chain, interval time.Duration) {
	rule, _ := finality.GetRule(chain)
	logger.Info("watching chain", "chain", chain, "confirmations", rule.Confirmations)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// TODO: Poll RPC, check finality, publish events
			_ = producer
		}
	}
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
