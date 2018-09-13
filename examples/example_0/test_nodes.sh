#!/bin/bash

HOST="localhost"
PORT="1883"

# Array for storing PIDS of future processes
PIDS=()

# Start the two Python processes and store the PIDs
timeout -k 15s 15s python3 test_node.py -host $HOST -port $PORT node_desc_b.json -val -1 &
PIDS+=("$!")
timeout -k 15s 15s python3 test_node.py -host $HOST -port $PORT node_desc_a.json -val 0 &
PIDS+=("$!")

# Kill all the processes we started
for i in "${PIDS[@]}"
do
	wait $i || kill -9 $i
done
