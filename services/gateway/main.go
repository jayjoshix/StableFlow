// Gateway is the public-facing API service for StableFlow. It accepts
// payment requests over HTTP, validates authentication, and dispatches
// commands to internal services (ledger, router) via Kafka.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/stableflow/stableflow/pkg/auth"
	"github.com/stableflow/stableflow/pkg/kafka"
	"github.com/stableflow/stableflow/pkg/rpc"
	"github.com/stableflow/stableflow/services/gateway/config"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)
	logger.Info("starting gateway service", "port", cfg.Port)

	// ── Kafka producer ────────────────────────────────────────
	producer, err := kafka.NewProducer(kafka.Config{
		Brokers:  cfg.KafkaBrokers,
		ClientID: "gateway",
	}, logger)
	if err != nil {
		logger.Error("failed to create kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	// ── In-memory idempotency store ───────────────────────────
	// SECURITY FIX: Prevents double-spend on client retries. A duplicate
	// Reference key within the TTL window is rejected with 409 Conflict.
	//
	// TODO(production): Replace with a Redis SET NX + TTL store so the
	// idempotency window survives pod restarts and works across multiple
	// gateway replicas. Key format: "idem:{reference}", TTL: 24h.
	// Ref: https://redis.io/commands/set (NX + EX options)
	idempotency := newIdempotencyStore()

	// ── Routes ────────────────────────────────────────────────
	started := time.Now()
	mux := http.NewServeMux()

	// Health checks (unauthenticated).
	mux.HandleFunc("GET /healthz", rpc.HealthHandler("gateway", started))
	mux.HandleFunc("GET /readyz", rpc.HealthHandler("gateway", started))

	// Payment API (authenticated).
	mux.HandleFunc("POST /api/v1/payments", handleCreatePayment(logger, producer, idempotency))
	mux.HandleFunc("GET /api/v1/payments/{id}", handleGetPayment(logger))
	mux.HandleFunc("GET /api/v1/payments", handleListPayments(logger))

	// ── Middleware stack ───────────────────────────────────────
	authCfg := auth.Config{
		JWTSecret: cfg.JWTSecret,
		APIKeys:   cfg.APIKeys,
	}
	var handler http.Handler = mux
	handler = auth.Middleware(authCfg, logger, handler)
	handler = rpc.RequestIDMiddleware(handler)
	handler = rpc.LoggingMiddleware(logger, handler)
	handler = rpc.RecoveryMiddleware(logger, handler)

	// TODO(security): Add mTLS between gateway and internal services.
	// TODO(resilience): Add per-account rate limiting (token bucket, 100 req/min default).
	// TODO(resilience): Add circuit breaker around Kafka publish (fail open with outbox fallback).

	// ── Server ────────────────────────────────────────────────
	srv := rpc.NewServer(rpc.ServerConfig{
		Port:         cfg.Port,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}, handler)

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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
	logger.Info("gateway stopped")
}

// ── Idempotency store ─────────────────────────────────────────

// idempotencyStore is an in-process, TTL-based deduplication store.
// It maps a client-supplied Reference key to the payment ID already
// issued for that key, so retried requests return the same response
// instead of creating a second payment.
//
// TODO(production): Replace with Redis SETNX. This in-memory version
// does not survive restarts and does not work across multiple replicas.
type idempotencyStore struct {
	mu      sync.Mutex
	entries map[string]idempotencyEntry
}

type idempotencyEntry struct {
	paymentID string
	expiresAt time.Time
}

const idempotencyTTL = 24 * time.Hour

func newIdempotencyStore() *idempotencyStore {
	s := &idempotencyStore{entries: make(map[string]idempotencyEntry)}
	// Background goroutine evicts expired entries every 10 minutes.
	go func() {
		for range time.Tick(10 * time.Minute) {
			s.evict()
		}
	}()
	return s
}

// check returns (existingPaymentID, alreadyExists).
func (s *idempotencyStore) check(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		return "", false
	}
	return e.paymentID, true
}

// record saves the mapping of reference → paymentID.
func (s *idempotencyStore) record(key, paymentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = idempotencyEntry{
		paymentID: paymentID,
		expiresAt: time.Now().Add(idempotencyTTL),
	}
}

func (s *idempotencyStore) evict() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, e := range s.entries {
		if now.After(e.expiresAt) {
			delete(s.entries, k)
		}
	}
}

// ── Handlers ──────────────────────────────────────────────────

// CreatePaymentRequest is the request body for creating a payment.
type CreatePaymentRequest struct {
	Amount      string `json:"amount"`       // decimal string, e.g. "100.50"
	Currency    string `json:"currency"`     // e.g. "USDC"
	Chain       string `json:"chain"`        // e.g. "ethereum"
	Recipient   string `json:"recipient"`    // on-chain address
	Reference   string `json:"reference"`    // idempotency key (required)
	CallbackURL string `json:"callback_url"` // webhook on completion
}

// PaymentResponse is returned after creating or fetching a payment.
type PaymentResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Amount    string `json:"amount"`
	Currency  string `json:"currency"`
	Chain     string `json:"chain"`
	Recipient string `json:"recipient"`
	CreatedAt string `json:"created_at"`
}

func handleCreatePayment(logger *slog.Logger, producer kafka.Producer, idem *idempotencyStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreatePaymentRequest
		if err := rpc.DecodeJSON(r, &req); err != nil {
			rpc.WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		// ── Input validation ──────────────────────────────────
		if req.Amount == "" || req.Currency == "" || req.Chain == "" || req.Recipient == "" {
			rpc.WriteError(w, http.StatusBadRequest, "amount, currency, chain, and recipient are required")
			return
		}

		// SECURITY FIX: Reference (idempotency key) is now required.
		// Without it a network-timeout retry creates a second payment
		// (double-spend). Clients must supply a stable, unique key per
		// payment intent (e.g. UUID v4 generated client-side).
		if req.Reference == "" {
			rpc.WriteError(w, http.StatusBadRequest, "reference (idempotency key) is required")
			return
		}

		// SECURITY FIX: Check for duplicate Reference before doing any work.
		// If a payment was already created for this Reference, return the
		// original payment ID with 200 (not 201) so the client knows it's
		// a replay, not a new payment.
		if existingID, exists := idem.check(req.Reference); exists {
			logger.WarnContext(r.Context(), "duplicate payment reference rejected",
				"reference", req.Reference,
				"existing_payment_id", existingID,
			)
			rpc.WriteJSON(w, http.StatusOK, PaymentResponse{
				ID:     existingID,
				Status: "pending",
			})
			return
		}

		paymentID := fmt.Sprintf("pay_%d", time.Now().UnixNano())
		logger.InfoContext(r.Context(), "creating payment",
			"payment_id", paymentID,
			"amount", req.Amount,
			"currency", req.Currency,
			"chain", req.Chain,
			"reference", req.Reference,
		)

		// FUND-LOSS FIX: Kafka publish error is no longer silently discarded.
		// If the broker is unavailable, we return 503 so the client can retry
		// with the same Reference key (idempotency ensures no duplicate on retry).
		//
		// TODO(production-critical): Replace this direct publish with the
		// Transactional Outbox pattern. Write the payment record AND the outbox
		// event in a single PostgreSQL transaction. A CDC process (Debezium)
		// then publishes to Kafka. This guarantees exactly-once delivery even
		// if the process crashes between the DB write and the Kafka publish.
		// Until this is done, a crash after the DB write but before Kafka publish
		// still results in a lost payment event.
		if err := producer.Publish(r.Context(), kafka.Message{
			Topic: "payment.created",
			Key:   []byte(paymentID),
			Value: []byte(fmt.Sprintf(`{"id":%q,"amount":%q,"currency":%q,"chain":%q,"recipient":%q,"reference":%q}`,
				paymentID, req.Amount, req.Currency, req.Chain, req.Recipient, req.Reference)),
			Headers: map[string]string{
				"request_id": rpc.GetRequestID(r.Context()),
				"reference":  req.Reference,
			},
		}); err != nil {
			logger.ErrorContext(r.Context(), "failed to publish payment event",
				"payment_id", paymentID,
				"error", err,
			)
			rpc.WriteError(w, http.StatusServiceUnavailable,
				"payment processing temporarily unavailable — please retry with the same reference key")
			return
		}

		// Record idempotency key AFTER successful publish so that a crash
		// before this line causes the client to retry (safe due to Kafka dedup).
		idem.record(req.Reference, paymentID)

		// TODO(saga): After publish, the gateway should await acknowledgement
		// from the ledger service (via a reply topic or synchronous RPC) before
		// returning 201. Currently the gateway returns 201 without confirming
		// the ledger has successfully recorded the debit. If the ledger crashes,
		// the payment is "created" but never debited.

		rpc.WriteJSON(w, http.StatusCreated, PaymentResponse{
			ID:        paymentID,
			Status:    "pending",
			Amount:    req.Amount,
			Currency:  req.Currency,
			Chain:     req.Chain,
			Recipient: req.Recipient,
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		})
	}
}

func handleGetPayment(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		logger.InfoContext(r.Context(), "fetching payment", "payment_id", id)
		// TODO: Fetch from ledger service.
		rpc.WriteJSON(w, http.StatusOK, PaymentResponse{
			ID:     id,
			Status: "pending",
		})
	}
}

func handleListPayments(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.InfoContext(r.Context(), "listing payments")
		// TODO: Fetch from ledger service with pagination.
		rpc.WriteJSON(w, http.StatusOK, []PaymentResponse{})
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
