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

# Runtime stage - minimal image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=go-builder /orchestrator /orchestrator

ENTRYPOINT ["/orchestrator"]
