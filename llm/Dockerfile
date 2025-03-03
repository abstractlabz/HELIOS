# Dockerfile.llm
FROM golang:1.19-alpine AS builder

# Install git and build dependencies
RUN apk add --no-cache git gcc musl-dev

# Set working directory
WORKDIR /app

# Initialize a new module
RUN echo "module github.com/0xPCDefenders/HELIOS" > go.mod
RUN echo "go 1.19" >> go.mod

# Install all required dependencies first
RUN go get github.com/gin-gonic/gin && \
    go get github.com/gorilla/websocket && \
    go get github.com/pinecone-io/go-pinecone/pinecone && \
    go get go.mongodb.org/mongo-driver/bson && \
    go get go.mongodb.org/mongo-driver/mongo && \
    go get go.mongodb.org/mongo-driver/mongo/options && \
    go get github.com/joho/godotenv && \
    go get github.com/google/uuid && \
    go get github.com/segmentio/kafka-go

# Copy all the source code
COPY . .

# Build the LLM components
RUN go build -o /chatbot ./chatbot
RUN go build -o /inferencer ./inferencer
RUN go build -o /ingestor ./ingestor

# Create a minimal runtime image
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy the built binaries from the builder stage
COPY --from=builder /chatbot /app/
COPY --from=builder /inferencer /app/
COPY --from=builder /ingestor /app/

# Create a simple entrypoint script that doesn't rely on config files
RUN echo '#!/bin/sh\n\
# Determine which service to start based on SERVICE_TYPE environment variable\n\
case "$SERVICE_TYPE" in\n\
  "chatbot")\n\
    echo "Starting Chatbot service..."\n\
    exec /app/chatbot\n\
    ;;\n\
  "inferencer")\n\
    echo "Starting Inferencer service..."\n\
    exec /app/inferencer\n\
    ;;\n\
  "ingestor")\n\
    echo "Starting Ingestor service..."\n\
    exec /app/ingestor\n\
    ;;\n\
  *)\n\
    echo "Please specify a valid SERVICE_TYPE (chatbot, inferencer, ingestor)"\n\
    exit 1\n\
    ;;\n\
esac' > /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

# Expose port
EXPOSE 8080

ENTRYPOINT ["/app/entrypoint.sh"]