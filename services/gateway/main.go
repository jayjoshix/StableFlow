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

	// ── Routes ────────────────────────────────────────────────
	started := time.Now()
	mux := http.NewServeMux()

	// Health checks (unauthenticated).
	mux.HandleFunc("GET /healthz", rpc.HealthHandler("gateway", started))
	mux.HandleFunc("GET /readyz", rpc.HealthHandler("gateway", started))

	// Payment API (authenticated).
	mux.HandleFunc("POST /api/v1/payments", handleCreatePayment(logger, producer))
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

// ── Handlers ──────────────────────────────────────────────────

// CreatePaymentRequest is the request body for creating a payment.
type CreatePaymentRequest struct {
	Amount      string `json:"amount"`       // decimal string, e.g. "100.50"
	Currency    string `json:"currency"`     // e.g. "USDC"
	Chain       string `json:"chain"`        // e.g. "ethereum"
	Recipient   string `json:"recipient"`    // on-chain address
	Reference   string `json:"reference"`    // idempotency key
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

func handleCreatePayment(logger *slog.Logger, producer kafka.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreatePaymentRequest
		if err := rpc.DecodeJSON(r, &req); err != nil {
			rpc.WriteError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.Amount == "" || req.Currency == "" || req.Chain == "" || req.Recipient == "" {
			rpc.WriteError(w, http.StatusBadRequest, "amount, currency, chain, and recipient are required")
			return
		}

		paymentID := fmt.Sprintf("pay_%d", time.Now().UnixNano())
		logger.InfoContext(r.Context(), "creating payment",
			"payment_id", paymentID,
			"amount", req.Amount,
			"currency", req.Currency,
			"chain", req.Chain,
		)

		// Publish payment.created event to Kafka.
		_ = producer.Publish(r.Context(), kafka.Message{
			Topic: "payment.created",
			Key:   []byte(paymentID),
			Value: []byte(fmt.Sprintf(`{"id":%q,"amount":%q,"currency":%q,"chain":%q,"recipient":%q}`,
				paymentID, req.Amount, req.Currency, req.Chain, req.Recipient)),
			Headers: map[string]string{
				"request_id": rpc.GetRequestID(r.Context()),
			},
		})

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
