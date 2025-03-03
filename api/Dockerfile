# Dockerfile.api
FROM golang:1.19-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev

# Set working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy utils (needed by all services)
COPY utils/ utils/

# Copy API, aggregation, scheduler, and cmd components
COPY api/ api/
COPY aggregation/ aggregation/
COPY scheduler/ scheduler/
COPY cmd/ cmd/

# Build all the services
RUN go build -o /api-server ./api
RUN go build -o /aggregator ./aggregation
RUN go build -o /scheduler ./scheduler
RUN go build -o /commander ./cmd

# Create a minimal runtime image
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binaries from builder stage
COPY --from=builder /api-server /app/
COPY --from=builder /aggregator /app/
COPY --from=builder /scheduler /app/
COPY --from=builder /commander /app/

# Create volumes for configuration and data
VOLUME ["/app/config", "/app/data"]

# Copy config files
COPY api/config/ /app/config/
COPY aggregation/config/ /app/config/
COPY scheduler/config/ /app/config/

# Set environment variables
ENV CONFIG_PATH="/app/config/.env"
ENV DATA_PATH="/app/data"

# Expose API port
EXPOSE 8080

# Create an entrypoint script to decide which service to run
COPY api/entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]