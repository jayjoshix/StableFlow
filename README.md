# StableFlow

**Stablecoin payment infrastructure** — a Go monorepo for processing, routing, and settling stablecoin payments across multiple blockchains.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Gateway    │────▶│   Router     │────▶│   Signer     │
│  (HTTP API)  │     │ (chain pick) │     │  (KMS/HSM)   │
└──────┬───────┘     └──────────────┘     └──────┬───────┘
       │                                         │
       ▼                                         ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Ledger     │◀───▶│  Listener    │     │  Reconciler  │
│ (dbl-entry)  │     │ (on-chain)   │     │  (verify)    │
└──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │
       └────────────────────┴────────────────────┘
                     Kafka (Redpanda)
```

## Services

| Service | Port | Description |
|---|---|---|
| **gateway** | 8080 | HTTP API — accepts payment requests, validates auth |
| **ledger** | 8081 | Double-entry accounting — records debits/credits in PostgreSQL |
| **listener** | — | Blockchain event watcher — monitors on-chain confirmations |
| **router** | 8083 | Payment routing — selects optimal chain for settlement |
| **signer** | 8084 | Transaction signing — KMS/HSM integration |
| **reconciler** | — | Reconciliation worker — cross-checks ledger vs chain state |

## Shared Packages

| Package | Purpose |
|---|---|
| `pkg/kafka` | Kafka producer/consumer abstractions |
| `pkg/rpc` | HTTP server helpers, JSON utilities, middleware |
| `pkg/finality` | Blockchain finality rules per chain |
| `pkg/auth` | JWT and API-key authentication middleware |

## Quick Start

```bash
# 1. Clone and enter
git clone <repo-url> && cd stableflow

# 2. Start infrastructure
docker compose up -d

# 3. Build all services
go build ./...

# 4. Run a service
go run ./services/gateway

# 5. Health check
curl http://localhost:8080/healthz
```

## Project Structure

```
stableflow/
├── go.work                 # Go workspace
├── docker-compose.yml      # PostgreSQL, Redpanda, Redis
├── Makefile                # Build, test, lint targets
├── pkg/
│   ├── auth/              # JWT + API-key middleware
│   ├── finality/          # Chain finality rules
│   ├── kafka/             # Producer/consumer wrappers
│   └── rpc/               # HTTP server + middleware
└── services/
    ├── gateway/           # Public HTTP API
    ├── ledger/            # Double-entry ledger
    ├── listener/          # Blockchain watcher
    ├── reconciler/        # Ledger ↔ chain reconciliation
    ├── router/            # Payment routing
    └── signer/            # Transaction signing
```

## Development

```bash
# Build all
make build

# Test all
make test

# Lint
make lint

# Tidy all modules
make tidy

# Sync workspace
make sync
```

## License

Proprietary — All rights reserved.
