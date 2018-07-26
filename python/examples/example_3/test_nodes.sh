#!/bin/bash

HOST="localhost"
PORT="1883"

# Array for storing PIDS of future processes
PIDS=()

# Start the two Python processes and store the PIDs of the timeouts.  Timeouts make sure that the process is killed after the specified duration
timeout -k 15s 15s python3 plant.py -host $HOST -port $PORT node_desc_plant.json &
PIDS+=("$!")
timeout -k 15s 15s python3 controller.py -host $HOST -port $PORT node_desc_controller.json &
PIDS+=("$!")

# Kill all the processes we started
for i in "${PIDS[@]}"; do
	wait $i || kill -9 $i
done
