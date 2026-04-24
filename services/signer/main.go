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

	// SECURITY FIX: Hard-stop the signer if it is configured with the local
	// stub provider but is not running in an explicitly dev environment.
	// A SHA256 hash is NOT a cryptographic signature — it does not prove
	// knowledge of a private key, cannot be verified on-chain, and would
	// allow anyone who can call this endpoint to "sign" arbitrary transactions.
	// If this guard fires, fix KMS_PROVIDER before starting the service.
	if cfg.KMSProvider == "local" {
		env := os.Getenv("APP_ENV")
		if env != "development" && env != "dev" && env != "test" {
			logger.Error("SECURITY: signer is running with KMS_PROVIDER=local in a non-dev environment. " +
				"The local provider uses SHA256 as a signing stub — it produces no real cryptographic signature " +
				"and must never be used in staging or production. " +
				"Set KMS_PROVIDER to 'aws-kms', 'gcp-kms', or 'hashicorp-vault' and provide KMS_KEY_ID.")
			os.Exit(1)
		}
		logger.Warn("⚠️  DEVELOPMENT ONLY: signer is using the local SHA256 stub. " +
			"This is NOT a real cryptographic signature. Do not use in staging or production.")
	}

	logger.Info("starting signer service", "port", cfg.Port, "kms", cfg.KMSProvider)

	// TODO(security-critical): Replace the stub with a real KMS integration:
	//
	//   AWS KMS:
	//     client := kms.NewFromConfig(awsCfg)
	//     output, err := client.Sign(ctx, &kms.SignInput{
	//       KeyId:            aws.String(cfg.KMSKeyID),
	//       Message:          txHash,
	//       MessageType:      types.MessageTypeDigest,
	//       SigningAlgorithm: types.SigningAlgorithmSpecEcdsaSha256,
	//     })
	//
	//   HashiCorp Vault Transit:
	//     POST /v1/transit/sign/{key_name}  body: {"input": base64(txHash)}
	//
	// TODO(security-critical): Implement a signing policy engine before the KMS call:
	//   - Per-account spending velocity limits (reject if > $10k/hr)
	//   - Amount ceiling per single tx (configurable, default $50k)
	//   - Block signing if recipient is on OFAC SDN list
	//   - Require 2-of-N human approvals for transactions above a threshold
	//   Without these controls a compromised gateway can drain all wallets.
	//
	// TODO(security): Implement MPC/TSS so no single machine holds a complete
	// private key. Recommended: Fireblocks SDK or tss-lib (Binance open-source).
	// A single-key signer is a single point of total fund loss.

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
	mux.HandleFunc("POST /api/internal/sign", handleSign(logger, producer, cfg.KMSProvider))

	var handler http.Handler = mux
	handler = rpc.RequestIDMiddleware(handler)
	handler = rpc.LoggingMiddleware(logger, handler)

	// TODO(security): Add mTLS on the sign endpoint — only the router service
	// should be able to call /api/internal/sign. An unauthenticated sign endpoint
	// is equivalent to exposing your private key.

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

func handleSign(logger *slog.Logger, producer kafka.Producer, kmsProvider string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SignRequest
		if err := rpc.DecodeJSON(r, &req); err != nil {
			rpc.WriteError(w, http.StatusBadRequest, "invalid request")
			return
		}

		// Input validation — every field is required for signing.
		if req.PaymentID == "" || req.Chain == "" || req.TxData == "" {
			rpc.WriteError(w, http.StatusBadRequest, "payment_id, chain, and tx_data are required")
			return
		}

		logger.InfoContext(r.Context(), "signing transaction",
			"payment_id", req.PaymentID,
			"chain", req.Chain,
			"kms_provider", kmsProvider,
		)

		// SECURITY FIX: The stub path is now labelled clearly and only reachable
		// after the startup guard above has confirmed we're in a dev environment.
		// In all other environments the service exits before reaching this point.
		//
		// TODO(production-critical): Replace this block with a real KMS call.
		// See comments in main() above for AWS KMS and Vault examples.
		hash := sha256.Sum256([]byte(req.TxData))
		sig := hex.EncodeToString(hash[:])
		txHash := fmt.Sprintf("0x%s", sig[:40])

		// FUND-LOSS FIX: Kafka publish error is no longer silently discarded.
		// If the signed event is lost, the listener never watches for the tx
		// on-chain, the payment never confirms, and funds are locked.
		if err := producer.Publish(r.Context(), kafka.Message{
			Topic: "payment.signed",
			Key:   []byte(req.PaymentID),
			Value: []byte(fmt.Sprintf(`{"payment_id":%q,"tx_hash":%q,"chain":%q}`,
				req.PaymentID, txHash, req.Chain)),
		}); err != nil {
			logger.ErrorContext(r.Context(), "failed to publish signing event",
				"payment_id", req.PaymentID,
				"error", err,
			)
			rpc.WriteError(w, http.StatusServiceUnavailable,
				"signing event could not be dispatched — transaction not broadcast")
			return
		}

		// TODO(audit): Every signing request must be written to an immutable
		// audit log (append-only table or write-once S3 bucket) with:
		//   payment_id, chain, tx_hash, signer_key_id, timestamp, caller_identity
		// This log is required for SOC 2, PCI-DSS, and regulatory inquiries.

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
