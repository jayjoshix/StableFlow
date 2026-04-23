FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /workspace
COPY go.work go.work
COPY pkg/ pkg/
COPY services/ledger/ services/ledger/
RUN cd services/ledger && go build -ldflags="-s -w" -o /bin/ledger .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/ledger /ledger
EXPOSE 8081
ENTRYPOINT ["/ledger"]
