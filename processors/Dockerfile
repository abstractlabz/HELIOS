# Dockerfile.processors
FROM golang:1.19-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy utils (needed by all containers)
COPY utils/ utils/

# Copy the processors and data components
COPY processors/ processors/
COPY data/ data/

# Build the processor services
RUN go build -o /data-processor ./processors/data
RUN go build -o /stream-processor ./processors/stream
RUN go build -o /finance-processor ./processors/finance

# Create the runtime image
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binaries from builder stage
COPY --from=builder /data-processor /app/
COPY --from=builder /stream-processor /app/
COPY --from=builder /finance-processor /app/

# Set up volumes for configuration and data
VOLUME ["/app/config", "/app/data"]

# Copy configuration files
COPY processors/config/ /app/config/

# Set environment variables
ENV CONFIG_PATH="/app/config/.env"
ENV DATA_PATH="/app/data"

# Create an entrypoint script to decide which processor to run
COPY processors/entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]