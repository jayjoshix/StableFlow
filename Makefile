.PHONY: build test lint fmt docker-up docker-down tidy sync

SERVICES := gateway ledger listener router signer reconciler
PACKAGES := kafka rpc finality auth

# ── Build ──────────────────────────────────────────────────────
build:
	@for svc in $(SERVICES); do \
		echo "→ building $$svc"; \
		cd services/$$svc && go build -o ../../bin/$$svc ./... && cd ../..; \
	done

build-%:
	cd services/$* && go build -o ../../bin/$* ./...

# ── Test ───────────────────────────────────────────────────────
test:
	go test ./...

test-%:
	cd services/$* && go test ./...

# ── Lint & Format ─────────────────────────────────────────────
lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

# ── Go Workspace ──────────────────────────────────────────────
tidy:
	@for svc in $(SERVICES); do \
		echo "→ tidy services/$$svc"; \
		cd services/$$svc && go mod tidy && cd ../..; \
	done
	@for pkg in $(PACKAGES); do \
		echo "→ tidy pkg/$$pkg"; \
		cd pkg/$$pkg && go mod tidy && cd ../..; \
	done

sync:
	go work sync

# ── Docker ─────────────────────────────────────────────────────
docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

docker-reset:
	docker compose down -v
	docker compose up -d
