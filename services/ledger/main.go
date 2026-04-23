// Ledger is the double-entry accounting service for StableFlow. It records
// debits and credits in PostgreSQL, consumes events from Kafka, and exposes
// an internal HTTP API for balance queries and journal entry creation.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stableflow/stableflow/pkg/kafka"
	"github.com/stableflow/stableflow/pkg/rpc"
	"github.com/stableflow/stableflow/services/ledger/config"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)
	logger.Info("starting ledger service",
		"port", cfg.Port,
		"database", maskDSN(cfg.DatabaseURL),
	)

	// ── Kafka consumer ────────────────────────────────────────
	consumer, err := kafka.NewConsumer(kafka.Config{
		Brokers:  cfg.KafkaBrokers,
		GroupID:  cfg.KafkaGroupID,
		ClientID: "ledger",
	}, logger)
	if err != nil {
		logger.Error("failed to create kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// ── Database ──────────────────────────────────────────────
	// TODO: Initialize PostgreSQL connection pool (pgxpool).
	logger.Info("database connection placeholder ready")

	// ── Routes ────────────────────────────────────────────────
	started := time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", rpc.HealthHandler("ledger", started))
	mux.HandleFunc("GET /readyz", rpc.HealthHandler("ledger", started))

	// Internal API
	mux.HandleFunc("POST /api/internal/entries", handleCreateEntry(logger))
	mux.HandleFunc("GET /api/internal/balances/{account}", handleGetBalance(logger))
	mux.HandleFunc("GET /api/internal/journal/{id}", handleGetJournalEntry(logger))

	var handler http.Handler = mux
	handler = rpc.RequestIDMiddleware(handler)
	handler = rpc.LoggingMiddleware(logger, handler)
	handler = rpc.RecoveryMiddleware(logger, handler)

	srv := rpc.NewServer(rpc.ServerConfig{
		Port:         cfg.Port,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}, handler)

	// ── Start Kafka consumer in background ────────────────────
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	go func() {
		if err := consumer.Subscribe(consumerCtx, []string{
			"payment.created",
			"payment.confirmed",
			"payment.failed",
		}, func(ctx context.Context, msg kafka.Message) error {
			logger.InfoContext(ctx, "processing event",
				"topic", msg.Topic,
				"key", string(msg.Key),
			)
			// TODO: Record journal entries in PostgreSQL.
			return nil
		}); err != nil {
			logger.Error("consumer error", "error", err)
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		logger.Info("received shutdown signal", "signal", sig)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
		}
	}

	consumerCancel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
	logger.Info("ledger stopped")
}

// ── Handlers ──────────────────────────────────────────────────

// EntryRequest represents a journal entry creation request.
type EntryRequest struct {
	PaymentID string `json:"payment_id"`
	DebitAcc  string `json:"debit_account"`
	CreditAcc string `json:"credit_account"`
	Amount    string `json:"amount"`
	Currency  string `json:"currency"`
	Memo      string `json:"memo"`
}

func handleCreateEntry(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req EntryRequest
		if err := rpc.DecodeJSON(r, &req); err != nil {
			rpc.WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		logger.InfoContext(r.Context(), "creating journal entry",
			"payment_id", req.PaymentID,
			"amount", req.Amount,
		)
		// TODO: Insert into PostgreSQL with double-entry validation.
		rpc.WriteJSON(w, http.StatusCreated, map[string]string{
			"status": "created",
			"id":     req.PaymentID,
		})
	}
}

func handleGetBalance(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		account := r.PathValue("account")
		logger.InfoContext(r.Context(), "fetching balance", "account", account)
		// TODO: Query balance from PostgreSQL.
		rpc.WriteJSON(w, http.StatusOK, map[string]string{
			"account": account,
			"balance": "0.00",
		})
	}
}

func handleGetJournalEntry(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		logger.InfoContext(r.Context(), "fetching journal entry", "id", id)
		// TODO: Query from PostgreSQL.
		rpc.WriteJSON(w, http.StatusOK, map[string]string{
			"id":     id,
			"status": "not_found",
		})
	}
}

// maskDSN hides the password in a DSN for safe logging.
func maskDSN(dsn string) string {
	if len(dsn) > 20 {
		return dsn[:20] + "***"
	}
	return "***"
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
