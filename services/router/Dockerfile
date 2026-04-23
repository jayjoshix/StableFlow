FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /workspace
COPY go.work go.work
COPY pkg/ pkg/
COPY services/router/ services/router/
RUN cd services/router && go build -ldflags="-s -w" -o /bin/router .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/router /router
EXPOSE 8083
ENTRYPOINT ["/router"]
