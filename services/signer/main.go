// Signer handles transaction signing with KMS/HSM integration.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stableflow/stableflow/pkg/kafka"
	"github.com/stableflow/stableflow/pkg/rpc"
	"github.com/stableflow/stableflow/services/signer/config"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)
	logger.Info("starting signer service", "port", cfg.Port, "kms", cfg.KMSProvider)

	producer, err := kafka.NewProducer(kafka.Config{
		Brokers:  cfg.KafkaBrokers,
		ClientID: "signer",
	}, logger)
	if err != nil {
		logger.Error("failed to create kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	started := time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", rpc.HealthHandler("signer", started))
	mux.HandleFunc("POST /api/internal/sign", handleSign(logger, producer))

	var handler http.Handler = mux
	handler = rpc.RequestIDMiddleware(handler)
	handler = rpc.LoggingMiddleware(logger, handler)

	srv := rpc.NewServer(rpc.DefaultServerConfig(cfg.Port), handler)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logger.Info("signer stopped")
}

type SignRequest struct {
	PaymentID string `json:"payment_id"`
	Chain     string `json:"chain"`
	TxData    string `json:"tx_data"` // hex-encoded unsigned tx
}

type SignResponse struct {
	PaymentID string `json:"payment_id"`
	Signature string `json:"signature"`
	TxHash    string `json:"tx_hash"`
}

func handleSign(logger *slog.Logger, producer kafka.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SignRequest
		if err := rpc.DecodeJSON(r, &req); err != nil {
			rpc.WriteError(w, http.StatusBadRequest, "invalid request")
			return
		}
		logger.InfoContext(r.Context(), "signing transaction",
			"payment_id", req.PaymentID, "chain", req.Chain,
		)
		// TODO: Replace with real KMS signing.
		hash := sha256.Sum256([]byte(req.TxData))
		sig := hex.EncodeToString(hash[:])
		txHash := fmt.Sprintf("0x%s", sig[:40])

		_ = producer.Publish(r.Context(), kafka.Message{
			Topic: "payment.signed",
			Key:   []byte(req.PaymentID),
			Value: []byte(fmt.Sprintf(`{"payment_id":%q,"tx_hash":%q}`, req.PaymentID, txHash)),
		})

		rpc.WriteJSON(w, http.StatusOK, SignResponse{
			PaymentID: req.PaymentID,
			Signature: sig,
			TxHash:    txHash,
		})
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
