# Build Go binary
FROM golang:1.25-alpine AS go-builder

WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY cmd/ ./cmd/
COPY pkg/ ./pkg/

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /orchestrator ./cmd/api

# Runtime stage - use Manticore image for indexer/searchd binaries
FROM manticoresearch/manticore:6.3.6

# Install ca-certificates for HTTPS calls
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the orchestrator binary
COPY --from=go-builder /orchestrator /orchestrator

# Override the default entrypoint (Manticore image has searchd as entrypoint)
ENTRYPOINT ["/orchestrator"]
