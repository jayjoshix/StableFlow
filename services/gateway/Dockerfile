# ── Build stage ─────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /workspace

# Copy workspace and dependency files first (cache-friendly).
COPY go.work go.work
COPY pkg/ pkg/
COPY services/gateway/ services/gateway/

# Build the binary.
RUN cd services/gateway && go build -ldflags="-s -w" -o /bin/gateway .

# ── Runtime stage ──────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/gateway /gateway
EXPOSE 8080
ENTRYPOINT ["/gateway"]
