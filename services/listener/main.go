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
	logger.Info("watching chain", "chain", chain, "confirmations_required", rule.Confirmations)

	// TODO(production-critical — FUND-LOSS): Implement chain reorg detection.
	// The current loop only checks confirmations once and declares a tx "final".
	// If a reorg occurs AFTER finality is declared, the ledger has credited funds
	// that no longer exist on the canonical chain (phantom money).
	//
	// Required implementation:
	//   1. Track the last N canonical block hashes per chain (N = confirmations_required + buffer).
	//   2. On each poll, compare the current block hash at each tracked height against
	//      the stored hash. A mismatch means a reorg occurred at that depth.
	//   3. For every tx that was "finalized" in a reorg'd block:
	//      a. Publish "payment.reorged" to Kafka
	//      b. Ledger consumes this event and issues a compensating journal entry
	//         to reverse the credit (moves payment back to "pending")
	//      c. Re-watch the tx — it may re-confirm on the new canonical chain
	//   4. Alert operators if the reorg depth exceeds confirmations_required
	//      (this indicates a possible 51% attack).
	//
	// Per-chain reorg risk (use higher confirmation thresholds for high-value payments):
	//   Ethereum  : 12 confirmations post-merge (Casper gadget provides ~2 epoch finality)
	//   Solana    : use "finalized" commitment level, not "confirmed"
	//   Tron      : 19 confirmations (DPOS, ~57 seconds)
	//   Polygon   : 128+ confirmations (higher reorg frequency than Ethereum mainnet)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// TODO(production-critical): Replace stub with real RPC polling.
			// For each chain:
			//   eth:  eth_getBlockByNumber("latest") + eth_getTransactionReceipt(txHash)
			//   sol:  getSignatureStatuses with searchTransactionHistory=true
			//   tron: /walletsolidity/gettransactionbyid
			//
			// Flow per pending tx:
			//   confirmations = latest_block - tx_block_number
			//   isFinal, _ := finality.IsFinal(chain, confirmations)
			//   if isFinal { publish "payment.confirmed" to Kafka }
			//
			// TODO(production-critical): Publish errors must not be silently
			// discarded here. A lost "payment.confirmed" event means the ledger
			// never settles the payment — funds are locked indefinitely.
			// Use a retry loop with exponential backoff (max 3 attempts), then
			// write to a dead-letter topic for manual intervention.
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
