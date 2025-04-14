#!/bin/bash

# Define the CSV file name
CSV_FILE="kubectl_latency.csv"

# Check if the CSV file exists; if not, create it with headers
if [ ! -f "$CSV_FILE" ]; then
    echo "timestamp,real_seconds,user_seconds,sys_seconds" > "$CSV_FILE"
fi

# Get the current timestamp
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Run the kubectl command and capture timing info
TIMING_INFO=$( { time kubectl get pods -A > /dev/null 2>&1; } 2>&1 )

# Function to convert time format (e.g., 0,086s or 0.086s) to seconds
convert_to_seconds() {
    local time_string=$1
    local minutes=$(echo "$time_string" | sed -n 's/\([0-9]*\)m.*/\1/p')
    local seconds=$(echo "$time_string" | sed -n 's/.*m\?\([0-9]*[.,][0-9]*\)s/\1/p' | tr "," ".")
    minutes=${minutes:-0}
    seconds=${seconds:-0}
    echo "scale=9; ($minutes * 60) + $seconds" | bc
}

# Extract and convert real, user, and sys times
REAL=$(echo "$TIMING_INFO" | grep 'real' | awk '{print $2}')
USER=$(echo "$TIMING_INFO" | grep 'user' | awk '{print $2}')
SYS=$(echo "$TIMING_INFO" | grep 'sys' | awk '{print $2}')

REAL_SECONDS=$(convert_to_seconds "$REAL")
USER_SECONDS=$(convert_to_seconds "$USER")
SYS_SECONDS=$(convert_to_seconds "$SYS")

# Append the timing data to the CSV file
echo "$TIMESTAMP,$REAL_SECONDS,$USER_SECONDS,$SYS_SECONDS" >> "$CSV_FILE"