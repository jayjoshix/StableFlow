FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /workspace
COPY go.work go.work
COPY pkg/ pkg/
COPY services/reconciler/ services/reconciler/
RUN cd services/reconciler && go build -ldflags="-s -w" -o /bin/reconciler .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/reconciler /reconciler
ENTRYPOINT ["/reconciler"]
