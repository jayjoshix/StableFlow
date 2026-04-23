FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /workspace
COPY go.work go.work
COPY pkg/ pkg/
COPY services/listener/ services/listener/
RUN cd services/listener && go build -ldflags="-s -w" -o /bin/listener .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/listener /listener
ENTRYPOINT ["/listener"]
