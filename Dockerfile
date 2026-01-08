FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod ./

# Download dependencies (none for now, but good practice)
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /orchestrator .

# Runtime stage - minimal image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /orchestrator /orchestrator

ENTRYPOINT ["/orchestrator"]
