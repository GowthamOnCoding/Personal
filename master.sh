#!/bin/bash
# parallel_remote_script_launcher.sh - Run multiple scripts with arguments on different servers simultaneously
# Usage: ./parallel_remote_script_launcher.sh

# Define servers, scripts, and their arguments
# Format: "username@server:/path/to/script.sh|arg1 arg2 arg3"
declare -a SERVER_SCRIPTS=(
  "user1@server1.example.com:/home/user1/scripts/script1.sh|--verbose --config=/etc/app1.conf"
  "user2@server2.example.com:/opt/scripts/script2.sh|-u admin -p 8080 --restart"
  "user3@server3.example.com:/var/scripts/script3.sh|-f /data/input.csv -o /data/output.json"
)

# SSH options (optional)
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

# Log directory
LOG_DIR="remote_logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"
SUMMARY_LOG="$LOG_DIR/summary.log"
echo "Starting parallel remote script execution at $(date)" > "$SUMMARY_LOG"

# Array to store background process PIDs
declare -a pids=()

# Function to run a script with arguments on a remote server
run_remote_script() {
  local server_script_args=$1
  local log_file=$2
  
  # Split the server_script and arguments
  local server_script=${server_script_args%%|*}
  local script_args=${server_script_args#*|}
  
  # If there's no pipe symbol, script_args will be equal to server_script_args
  if [ "$script_args" = "$server_script_args" ]; then
    script_args=""
  fi
  
  # Extract server and script details
  local server_user=${server_script%%@*}
  local rest=${server_script#*@}
  local server=${rest%%:*}
  local script=${rest#*:}
  
  echo "====================================" > "$log_file"
  echo "Executing on $server: $script $script_args" >> "$log_file"
  echo "Started at $(date)" >> "$log_file"
  
  # Run the script with arguments on the remote server
  ssh $SSH_OPTS "$server_user@$server" "bash -c '$script $script_args'" >> "$log_file" 2>&1
  
  local exit_status=$?
  echo "Completed at $(date)" >> "$log_file"
  echo "Exit status: $exit_status" >> "$log_file"
  
  # Write result to a special file for later processing
  echo "$server:$exit_status" > "$log_file.result"
}

# Main execution
echo "Starting parallel execution of scripts on remote servers..."

for server_script_args in "${SERVER_SCRIPTS[@]}"; do
  server=${server_script_args#*@}
  server=${server%%:*}
  
  # Create a unique log file for this server
  log_file="$LOG_DIR/$server.log"
  
  echo "Launching script on $server..."
  
  # Run the function in the background
  run_remote_script "$server_script_args" "$log_file" &
  
  # Store the PID of the background process
  pids+=($!)
done

echo "All scripts launched. Waiting for completion..."

# Wait for all background processes to complete
for pid in "${pids[@]}"; do
  wait $pid
done

# Collect results
echo -e "\nExecution summary:" | tee -a "$SUMMARY_LOG"
failed_servers=()

for result_file in "$LOG_DIR"/*.result; do
  if [ -f "$result_file" ]; then
    result=$(cat "$result_file")
    server=${result%%:*}
    exit_status=${result#*:}
    
    if [ "$exit_status" -eq 0 ]; then
      echo "$server: SUCCESS" | tee -a "$SUMMARY_LOG"
    else
      echo "$server: FAILED (exit code $exit_status)" | tee -a "$SUMMARY_LOG"
      failed_servers+=("$server")
    fi
  fi
done

# Final summary
echo -e "\nFinal summary:" | tee -a "$SUMMARY_LOG"
if [ ${#failed_servers[@]} -eq 0 ]; then
  echo "All scripts executed successfully." | tee -a "$SUMMARY_LOG"
else
  echo "The following servers had execution failures:" | tee -a "$SUMMARY_LOG"
  for server in "${failed_servers[@]}"; do
    echo "- $server" | tee -a "$SUMMARY_LOG"
  done
fi

echo -e "\nDetailed logs available in: $LOG_DIR/" | tee -a "$SUMMARY_LOG"
