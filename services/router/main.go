// Router selects the optimal chain and path for settling stablecoin
// payments based on fees, speed, and liquidity.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stableflow/stableflow/pkg/finality"
	"github.com/stableflow/stableflow/pkg/rpc"
	"github.com/stableflow/stableflow/services/router/config"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)
	logger.Info("starting router service", "port", cfg.Port)

	started := time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", rpc.HealthHandler("router", started))
	mux.HandleFunc("POST /api/internal/route", handleRoute(logger))
	mux.HandleFunc("GET /api/internal/chains", handleListChains(logger))

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
	logger.Info("router stopped")
}

type RouteRequest struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
	Chain    string `json:"chain,omitempty"` // optional preferred chain
}

type RouteResponse struct {
	Chain         string `json:"chain"`
	Confirmations int    `json:"confirmations"`
	EstFinality   string `json:"estimated_finality"`
	Score         int    `json:"score"` // routing score (lower is better)
}

func handleRoute(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req RouteRequest
		if err := rpc.DecodeJSON(r, &req); err != nil {
			rpc.WriteError(w, http.StatusBadRequest, "invalid request")
			return
		}
		// Simple routing: pick the fastest chain.
		best := finality.Solana
		if req.Chain != "" {
			best = finality.Chain(req.Chain)
		}
		rule, err := finality.GetRule(best)
		if err != nil {
			rpc.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		rpc.WriteJSON(w, http.StatusOK, RouteResponse{
			Chain:         string(rule.Chain),
			Confirmations: rule.Confirmations,
			EstFinality:   rule.EstimatedFinality.String(),
			Score:         rule.Confirmations,
		})
	}
}

func handleListChains(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chains := finality.Chains()
		out := make([]map[string]any, 0, len(chains))
		for _, c := range chains {
			rule, _ := finality.GetRule(c)
			out = append(out, map[string]any{
				"chain":         string(c),
				"confirmations": rule.Confirmations,
				"finality":      rule.EstimatedFinality.String(),
			})
		}
		rpc.WriteJSON(w, http.StatusOK, out)
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
