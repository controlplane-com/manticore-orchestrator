# Build Go binary
FROM golang:1.25-alpine AS go-builder

WORKDIR /app

# Copy go.work and all module files
COPY go.work ./
COPY api/go.mod ./api/
COPY agent/go.mod ./agent/
COPY shared/go.mod ./shared/

# Copy agent go.sum if it exists (needed for dependency resolution)
COPY agent/go.sum ./agent/

# Download dependencies
RUN cd api && go mod download

# Copy source code for all modules
COPY api/ ./api/
COPY agent/ ./agent/
COPY shared/ ./shared/

# Build the binary
RUN cd api && CGO_ENABLED=0 GOOS=linux go build -o /orchestrator .

# Runtime stage - minimal image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=go-builder /orchestrator /orchestrator

ENTRYPOINT ["/orchestrator"]
