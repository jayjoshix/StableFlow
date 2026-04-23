FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /workspace
COPY go.work go.work
COPY pkg/ pkg/
COPY services/signer/ services/signer/
RUN cd services/signer && go build -ldflags="-s -w" -o /bin/signer .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/signer /signer
EXPOSE 8084
ENTRYPOINT ["/signer"]
