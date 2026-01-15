# Build csv-to-manticore from GitHub
FROM golang:1.25-alpine AS csv-builder
RUN apk add --no-cache git
ARG CACHEBUST=4
WORKDIR /csv
RUN git clone https://github.com/Cuppojoe/csv-to-manticore.git .
RUN CGO_ENABLED=0 GOOS=linux go build -o /csv-to-manticore ./cmd/csv-to-manticore

# Build agent
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY cmd/ ./cmd/
COPY pkg/ ./pkg/

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /agent ./cmd/agent

# Runtime stage - use manticore image as base
FROM manticoresearch/manticore:latest

# Install mysql client for piping INSERT statements
RUN apt-get update && apt-get install -y default-mysql-client && rm -rf /var/lib/apt/lists/*

# Copy csv-to-manticore binary
COPY --from=csv-builder /csv-to-manticore /usr/local/bin/csv-to-manticore

# Copy agent binary
COPY --from=builder /agent /usr/local/bin/agent

# Default environment
ENV MYSQL_HOST=127.0.0.1
ENV MYSQL_PORT=9306
ENV SCHEMA_FILE=/etc/manticore/schema.conf
ENV CLUSTER_NAME=myc
ENV S3_MOUNT=/mnt/s3
ENV CSV_FOLDER=manticore-imports
ENV LISTEN_ADDR=:8080
ENV IMPORT_BATCH_SIZE=1000

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/agent"]
