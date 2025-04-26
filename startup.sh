#!/bin/sh

# Activate Python virtual environment
. /app/venv/bin/activate

# Create log files
mkdir -p /app/logs
touch /app/logs/aggregator.log
touch /app/logs/financials.log
touch /app/logs/news.log
touch /app/logs/inferencer.log
touch /app/logs/ingestor.log
touch /app/logs/chatbot.log
touch /app/logs/scheduler.log
touch /app/logs/upgrade.log
touch /app/logs/webhook.log
touch /app/logs/discord.log

# Function to start a service with logging
start_service() {
    service_name=$1
    service_dir=$2
    service_type=$3  # "go" or "python"
    log_file="/app/logs/${service_name}.log"
    echo "Starting ${service_name} service..."
    cd ${service_dir}
    if [ "$service_type" = "go" ]; then
        /app/bin/${service_name} >> ${log_file} 2>&1 &
    else
        if [ "$service_name" = "upgrade" ]; then
            # Run upgrade service with gunicorn in production mode
            gunicorn \
                --bind 0.0.0.0:5000 \
                --workers 4 \
                --worker-class sync \
                --timeout 120 \
                --keep-alive 5 \
                --certfile /app/api/client/server.crt \
                --keyfile /app/api/client/server.key \
                --access-logfile /app/logs/upgrade-access.log \
                --error-logfile /app/logs/upgrade-error.log \
                --log-level info \
                --ssl-version TLSv1_2 \
                upgrade:app >> ${log_file} 2>&1 &
        else
            python3 ${service_name}.py >> ${log_file} 2>&1 &
        fi
    fi
    sleep 2 # Give each service time to initialize
}

# Start all services
cd /app

# Start Go services
start_service "aggregator" "/app/cmd" "go"
start_service "financials" "/app/processors/financials" "go"
start_service "news" "/app/processors/news" "go"
start_service "inferencer" "/app/llm/inferencer" "go"
start_service "ingestor" "/app/llm/ingestor" "go"
start_service "chatbot" "/app/llm/chatbot" "go"
#start_service "scheduler" "/app/scheduler" "go"

# Start Python services
cd /app/api/client
start_service "upgrade" "/app/api/client" "python"
start_service "upgrade-webhook" "/app/api/client" "python"

cd /app/api/discord
start_service "bot-cli" "/app/api/discord" "python"

# Monitor all services
echo "All services started. Monitoring logs..."

# Tail all logs
tail -f /app/logs/*.log &

# Wait for any service to exit
wait

# If any service exits, log it and exit the container
echo "A service has exited. Shutting down container..."
exit 1
