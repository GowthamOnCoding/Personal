#!/bin/bash
# remote_script_launcher.sh - Run multiple scripts on different servers
# Usage: ./remote_script_launcher.sh

# Define servers and corresponding scripts
# Format: "username@server:/path/to/script.sh"
declare -a SERVER_SCRIPTS=(
  "user1@server1.example.com:/home/user1/scripts/script1.sh"
  "user2@server2.example.com:/opt/scripts/script2.sh"
  "user3@server3.example.com:/var/scripts/script3.sh"
)

# SSH options (optional)
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

# Log file
LOG_FILE="remote_execution_$(date +%Y%m%d_%H%M%S).log"
echo "Starting remote script execution at $(date)" > "$LOG_FILE"

# Function to run a script on a remote server
run_remote_script() {
  local server_script=$1
  local server_user=${server_script%%@*}
  local rest=${server_script#*@}
  local server=${rest%%:*}
  local script=${rest#*:}
  
  echo "====================================" >> "$LOG_FILE"
  echo "Executing on $server: $script" >> "$LOG_FILE"
  echo "Started at $(date)" >> "$LOG_FILE"
  
  # Run the script on the remote server
  ssh $SSH_OPTS "$server_user@$server" "bash -c '$script'" >> "$LOG_FILE" 2>&1
  
  local exit_status=$?
  echo "Completed at $(date)" >> "$LOG_FILE"
  echo "Exit status: $exit_status" >> "$LOG_FILE"
  
  return $exit_status
}

# Main execution
echo "Starting execution of scripts on remote servers..."
failed_servers=()

for server_script in "${SERVER_SCRIPTS[@]}"; do
  server=${server_script#*@}
  server=${server%%:*}
  
  echo -n "Running script on $server... "
  if run_remote_script "$server_script"; then
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
