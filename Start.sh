#!/bin/bash

# Usage: ./restart.sh <env> [port]
ENV=$1
PORT=${2:-8080}
JAR_NAME="your-app.jar"
LOG_FILE="app-$ENV.log"
PID_FILE="app-$ENV.pid"
JAVA_OPTS="-Xms256m -Xmx512m -Dspring.profiles.active=$ENV -Dserver.port=$PORT"

if [ -z "$ENV" ]; then
  echo "Usage: ./restart.sh <env> [port]"
  exit 1
fi

echo "Environment: $ENV"
echo "Port: $PORT"
echo "JAR: $JAR_NAME"
echo "Log: $LOG_FILE"

# Stop app if running
if [ -f "$PID_FILE" ]; then
  PID=$(cat $PID_FILE)
  echo "Stopping existing application with PID $PID..."
  kill $PID

  # Wait a bit and check if still running
  sleep 3
  if ps -p $PID > /dev/null; then
    echo "Process still alive, force killing..."
    kill -9 $PID
  else
    echo "Application stopped gracefully."
  fi

  rm $PID_FILE
else
  echo "No running application found for $ENV."
fi

# Start app
echo "Starting application..."
nohup java $JAVA_OPTS -jar $JAR_NAME > $LOG_FILE 2>&1 &
echo $! > $PID_FILE
echo "Started new application with PID $(cat $PID_FILE). Logs: $LOG_FILE"
