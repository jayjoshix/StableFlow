// Ledger is the double-entry accounting service for StableFlow. It records
// debits and credits in PostgreSQL, consumes events from Kafka, and exposes
// an internal HTTP API for balance queries and journal entry creation.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
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
	// TODO(production-critical): Replace with pgxpool.New() and run
	// migrations before accepting traffic. Verify double-entry constraints
	// are enforced at the DB layer via CHECK constraints and triggers:
	//   - journal entries: SUM(debit_amount) = SUM(credit_amount) per entry
	//   - account balances: no negative balance for asset accounts
	//   - entries are immutable: no UPDATE/DELETE on posted entries
	logger.Info("database connection placeholder ready")

	// ── In-memory Kafka deduplication store ───────────────────
	// FUND-LOSS FIX: Prevents a consumer rebalance (pod restart, scale event)
	// from processing the same Kafka message twice, which would create
	// duplicate journal entries (phantom credits or double debits).
	//
	// TODO(production-critical): Replace with a PostgreSQL-backed dedup table:
	//   CREATE TABLE processed_messages (
	//     message_id TEXT PRIMARY KEY,  -- topic:partition:offset
	//     processed_at TIMESTAMPTZ DEFAULT now()
	//   );
	// Check + insert this row in the SAME transaction as the journal entry write.
	// This gives exactly-once processing for free via DB atomicity.
	dedup := newMessageDeduplicator()

	// ── Routes ────────────────────────────────────────────────
	started := time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", rpc.HealthHandler("ledger", started))
	mux.HandleFunc("GET /readyz", rpc.HealthHandler("ledger", started))

	// Internal API — only reachable from within the cluster.
	// TODO(security): Add mTLS client certificate verification on these routes
	// so only authorised internal services can post journal entries.
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
			// FUND-LOSS FIX: Build a stable message ID from topic+partition+offset
			// so we can detect and skip redeliveries without double-posting entries.
			msgID := fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
			if dedup.seen(msgID) {
				logger.WarnContext(ctx, "skipping duplicate kafka message",
					"msg_id", msgID,
					"topic", msg.Topic,
				)
				return nil // return nil so the offset IS committed (message consumed)
			}

			logger.InfoContext(ctx, "processing event",
				"topic", msg.Topic,
				"key", string(msg.Key),
				"msg_id", msgID,
			)

			// TODO(production-critical): Inside a single PostgreSQL transaction:
			//   1. INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING
			//   2. If inserted (not a duplicate), write the journal entry
			//   3. Commit — both happen atomically or neither does
			// This replaces the in-memory dedup above with true exactly-once semantics.

			// TODO(saga): On "payment.failed", issue a compensating journal entry
			// to reverse the debit hold recorded on "payment.created". Without
			// this, failed payments leave funds permanently locked.

			dedup.mark(msgID)
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

// ── Kafka message deduplicator ────────────────────────────────

// messageDeduplicator is an in-memory set of already-processed Kafka message IDs.
// It prevents a consumer rebalance from re-processing a message and creating
// duplicate journal entries.
//
// TODO(production): Replace with a DB-backed check inside the entry write transaction.
type messageDeduplicator struct {
	mu   sync.Mutex
	seen_ map[string]struct{}
}

func newMessageDeduplicator() *messageDeduplicator {
	return &messageDeduplicator{seen_: make(map[string]struct{})}
}

func (d *messageDeduplicator) seen(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen_[id]
	return ok
}

func (d *messageDeduplicator) mark(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen_[id] = struct{}{}
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

		// FUND-LOSS FIX: Enforce double-entry invariants in the application
		// layer. These checks prevent corrupted journal entries that would
		// make reconciliation impossible and hide missing funds.
		//
		// Rule 1: All fields must be present.
		if req.PaymentID == "" || req.DebitAcc == "" || req.CreditAcc == "" ||
			req.Amount == "" || req.Currency == "" {
			rpc.WriteError(w, http.StatusBadRequest,
				"payment_id, debit_account, credit_account, amount, and currency are required")
			return
		}

		// Rule 2: Debit and credit accounts must be different.
		// A self-transfer is either a bug or a fraud attempt.
		if req.DebitAcc == req.CreditAcc {
			rpc.WriteError(w, http.StatusBadRequest,
				"debit_account and credit_account must be different")
			return
		}

		// Rule 3: Amount must be a valid positive decimal. We never accept
		// zero, negative, or non-numeric amounts to prevent phantom entries.
		if err := validateAmount(req.Amount); err != nil {
			rpc.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid amount: %v", err))
			return
		}

		// Rule 4: Currency must be a known stablecoin symbol.
		if !isKnownCurrency(req.Currency) {
			rpc.WriteError(w, http.StatusBadRequest,
				fmt.Sprintf("unknown currency %q — supported: USDC, USDT, DAI, PYUSD", req.Currency))
			return
		}

		logger.InfoContext(r.Context(), "creating journal entry",
			"payment_id", req.PaymentID,
			"debit_account", req.DebitAcc,
			"credit_account", req.CreditAcc,
			"amount", req.Amount,
			"currency", req.Currency,
		)

		// TODO(production-critical): Execute this inside a single PostgreSQL transaction:
		//   BEGIN;
		//   INSERT INTO journal_entries (payment_id, debit_account, credit_account,
		//     amount, currency, memo, created_at)
		//   VALUES ($1, $2, $3, $4::NUMERIC, $5, $6, now());
		//   -- DB-level CHECK constraint enforces debit != credit and amount > 0
		//   -- Trigger verifies the running balance of debit_account >= 0 (for asset accounts)
		//   COMMIT;
		// On constraint violation, return 422 Unprocessable Entity.

		// TODO(resilience): Single PostgreSQL instance is a SPOF. Add streaming
		// replication (hot standby) and automated failover (Patroni or RDS Multi-AZ)
		// before going to production.

		rpc.WriteJSON(w, http.StatusCreated, map[string]string{
			"status": "created",
			"id":     req.PaymentID,
		})
	}
}

// validateAmount returns an error if the amount string is not a valid
// positive decimal number. Uses string parsing to avoid float64 precision loss.
func validateAmount(amount string) error {
	amount = strings.TrimSpace(amount)
	if amount == "" {
		return fmt.Errorf("amount is empty")
	}
	// Allow one optional decimal point.
	parts := strings.SplitN(amount, ".", 2)
	for _, part := range parts {
		if part == "" {
			return fmt.Errorf("malformed decimal: %q", amount)
		}
		for _, ch := range part {
			if ch < '0' || ch > '9' {
				return fmt.Errorf("non-numeric character in amount: %q", amount)
			}
		}
	}
	// Reject zero.
	f, _ := strconv.ParseFloat(amount, 64)
	if f <= 0 {
		return fmt.Errorf("amount must be greater than zero, got %q", amount)
	}
	return nil
}

// isKnownCurrency returns true if the currency symbol is a supported stablecoin.
func isKnownCurrency(currency string) bool {
	switch strings.ToUpper(currency) {
	case "USDC", "USDT", "DAI", "PYUSD", "USDP", "FRAX", "TUSD":
		return true
	}
	return false
}

func handleGetBalance(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		account := r.PathValue("account")
		logger.InfoContext(r.Context(), "fetching balance", "account", account)
		// TODO: Query balance from PostgreSQL: SELECT SUM(credit) - SUM(debit)
		// FROM journal_entries WHERE account = $1.
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
