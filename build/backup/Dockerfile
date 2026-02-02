# Build Go binary
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY cmd/ ./cmd/
COPY pkg/ ./pkg/

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /backup ./cmd/backup

# Runtime stage - minimal Alpine image
FROM alpine:3.21

# Install ca-certificates for HTTPS calls to cloud storage APIs
RUN apk add --no-cache ca-certificates

# Copy the backup binary
COPY --from=builder /backup /usr/local/bin/backup

ENTRYPOINT ["/usr/local/bin/backup"]
