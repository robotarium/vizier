#!/bin/bash

HOST="localhost"
PORT="1883"

# Array for storing PIDS of future processes
PIDS=()

# Start the two Python processes and store the PIDs of the timeouts.  Timeouts make sure that the process is killed after the specified duration
timeout -k 15s 15s python3 test_a.py -host $HOST -port $PORT node_desc_b.json &
PIDS+=("$!")
timeout -k 15s 15s python3 test_b.py -host $HOST -port $PORT node_desc_a.json &
PIDS+=("$!")

# Kill all the processes we started
for i in "${PIDS[@]}"; do
	wait $i || kill -9 $i
done
