// Reconciler periodically cross-checks the ledger against on-chain
// state to detect and flag discrepancies.
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
	"github.com/stableflow/stableflow/pkg/rpc"
	"github.com/stableflow/stableflow/services/reconciler/config"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)
	logger.Info("starting reconciler", "interval", cfg.Interval, "batch", cfg.BatchSize)

	producer, err := kafka.NewProducer(kafka.Config{
		Brokers:  cfg.KafkaBrokers,
		ClientID: "reconciler",
	}, logger)
	if err != nil {
		logger.Error("failed to create kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go reconcileLoop(ctx, logger, producer, cfg)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("reconciler stopping")
	cancel()
	time.Sleep(2 * time.Second)
	logger.Info("reconciler stopped")
}

func reconcileLoop(ctx context.Context, logger *slog.Logger, producer kafka.Producer, cfg config.Config) {
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logger.Info("running reconciliation pass", "batch_size", cfg.BatchSize)
			runReconciliation(ctx, logger, producer, cfg)
		}
	}
}

func runReconciliation(ctx context.Context, logger *slog.Logger, producer kafka.Producer, cfg config.Config) {
	// TODO: Implementation steps:
	// 1. Query ledger DB for confirmed payments in batch
	// 2. For each payment, query on-chain state via RPC
	// 3. Compare amounts, status, finality
	// 4. Flag discrepancies via Kafka "reconciliation.mismatch" topic

	chains := finality.Chains()
	for _, chain := range chains {
		rule, _ := finality.GetRule(chain)
		logger.Debug("reconciling chain",
			"chain", chain,
			"confirmations_required", rule.Confirmations,
		)
		// TODO: Query and compare.
	}

	// Placeholder: use rpc package for potential health reporting
	_ = rpc.GetRequestID(ctx)
	_ = producer
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
