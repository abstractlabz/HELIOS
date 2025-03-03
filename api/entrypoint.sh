#!/bin/sh
# entrypoint.sh for LLM services

# Load environment variables from config file
if [ -f "$CONFIG_PATH" ]; then
  export $(cat $CONFIG_PATH | grep -v '^#' | xargs)
fi

# Determine which service to start based on SERVICE_TYPE environment variable
case "$SERVICE_TYPE" in
  "chatbot")
    echo "Starting Chatbot service..."
    exec /app/chatbot
    ;;
  "inferencer")
    echo "Starting Inferencer service..."
    exec /app/inferencer
    ;;
  "ingestor")
    echo "Starting Ingestor service..."
    exec /app/ingestor
    ;;
  *)
    echo "Please specify a valid SERVICE_TYPE (chatbot, inferencer, ingestor)"
    exit 1
    ;;
esac