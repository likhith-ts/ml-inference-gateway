# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Cache dependency downloads.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /gateway ./cmd/gateway

# ── Stage 2: minimal runtime image ────────────────────────────────────────────
FROM scratch

# Import CA certificates for outbound TLS (future use).
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /gateway /gateway

EXPOSE 50051 8080 2112

ENTRYPOINT ["/gateway"]
