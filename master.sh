#!/bin/bash
# remote_script_launcher.sh - Run multiple scripts with arguments on different servers
# Usage: ./remote_script_launcher.sh

# Define servers, scripts, and their arguments
# Format: "username@server:/path/to/script.sh|arg1 arg2 arg3"
# The pipe symbol (|) separates the script path from its arguments
declare -a SERVER_SCRIPTS=(
  "user1@server1.example.com:/home/user1/scripts/script1.sh|--verbose --config=/etc/app1.conf"
  "user2@server2.example.com:/opt/scripts/script2.sh|-u admin -p 8080 --restart"
  "user3@server3.example.com:/var/scripts/script3.sh|-f /data/input.csv -o /data/output.json"
)

# SSH options (optional)
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

# Log file
LOG_FILE="remote_execution_$(date +%Y%m%d_%H%M%S).log"
echo "Starting remote script execution at $(date)" > "$LOG_FILE"

# Function to run a script with arguments on a remote server
run_remote_script() {
  local server_script_args=$1
  
  # Split the server_script and arguments
  local server_script=${server_script_args%%|*}
  local script_args=${server_script_args#*|}
  
  # If there's no pipe symbol, script_args will be equal to server_script_args
  # so we need to check and set to empty if needed
  if [ "$script_args" = "$server_script_args" ]; then
    script_args=""
  fi
  
  # Extract server and script details
  local server_user=${server_script%%@*}
  local rest=${server_script#*@}
  local server=${rest%%:*}
  local script=${rest#*:}
  
  echo "====================================" >> "$LOG_FILE"
  echo "Executing on $server: $script $script_args" >> "$LOG_FILE"
  echo "Started at $(date)" >> "$LOG_FILE"
  
  # Run the script with arguments on the remote server
  # Properly quote the arguments to preserve spacing and special characters
  ssh $SSH_OPTS "$server_user@$server" "bash -c '$script $script_args'" >> "$LOG_FILE" 2>&1
  
  local exit_status=$?
  echo "Completed at $(date)" >> "$LOG_FILE"
  echo "Exit status: $exit_status" >> "$LOG_FILE"
  
  return $exit_status
}

# Main execution
echo "Starting execution of scripts on remote servers..."
failed_servers=()

for server_script_args in "${SERVER_SCRIPTS[@]}"; do
  server=${server_script_args#*@}
  server=${server%%:*}
  
  echo -n "Running script on $server... "
  if run_remote_script "$server_script_args"; then
    echo "SUCCESS"
  else
    echo "FAILED"
    failed_servers+=("$server")
  fi
done

# Summary
echo -e "\nExecution summary:"
if [ ${#failed_servers[@]} -eq 0 ]; then
  echo "All scripts executed successfully."
else
  echo "The following servers had execution failures:"
  for server in "${failed_servers[@]}"; do
    echo "- $server"
  done
  echo "Check $LOG_FILE for detailed logs."
fi

echo -e "\nFull execution log available in: $LOG_FILE"
