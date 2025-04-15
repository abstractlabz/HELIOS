# Use stable golang version
FROM golang:1.23-alpine

# Install Python and other dependencies
RUN apk add --no-cache gcc musl-dev git python3 py3-pip python3-dev

# Set working directory
WORKDIR /app

# Copy go mod files first (for better caching)
COPY go.mod go.sum ./

# Modify go.mod to use Go 1.23
RUN go mod edit -go=1.23

# Download Go dependencies
RUN go mod download

# Set up Python virtual environment
RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Copy Python requirements first
COPY api/client/requirements.txt ./api/client/
COPY api/discord/requirements.txt ./api/discord/

# Install Python dependencies in virtual environment
RUN . /app/venv/bin/activate && \
    pip3 install --no-cache-dir -r api/client/requirements.txt && \
    pip3 install --no-cache-dir -r api/discord/requirements.txt

# Copy entire codebase
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/config

# Set environment variables
ENV GIN_MODE=release
ENV CONFIG_PATH=/app/config 
ENV LOG_PATH=/app/logs
ENV KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS}
ENV KAFKA_KEY=${KAFKA_KEY}
ENV KAFKA_SECRET=${KAFKA_SECRET}
ENV HELIOS_API_KEY=${HELIOS_API_KEY}
ENV POLYGON_API_KEY=${POLYGON_API_KEY}
ENV OPENAI_API_KEY=${OPENAI_API_KEY}
ENV PINECONE_API_KEY=${PINECONE_API_KEY}
ENV PINECONE_HOST=${PINECONE_HOST}
ENV MONGO_URI=${MONGO_URI}
ENV DEEPSEEK_API_KEY=${DEEPSEEK_API_KEY}
ENV GOOGLE_API_KEY=${GOOGLE_API_KEY}
ENV GOOGLE_CSE_ID=${GOOGLE_CSE_ID}

# Build the Go services
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/aggregator ./cmd/main.go && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/bin/financials ./processors/financials/financials_processor.go && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/bin/news ./processors/news/news_processor.go && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/bin/inferencer ./llm/inferencer/inferencer.go && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/bin/ingestor ./llm/ingestor/ingestor.go && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/bin/chatbot ./llm/chatbot/chatbotquery.go && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/bin/scheduler ./scheduler/scheduler.go

# Make Python files executable
RUN chmod +x /app/api/client/upgrade.py && \
    chmod +x /app/api/client/upgrade-webhook.py && \
    chmod +x /app/api/discord/bot-cli.py

# Copy startup script
COPY startup.sh /app/
RUN chmod +x /app/startup.sh

# Expose necessary ports
EXPOSE 8081
EXPOSE 5000
EXPOSE 6002
EXPOSE 8035

# Run the services
CMD ["/app/startup.sh"]
