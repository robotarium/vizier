#!/bin/bash

# Give the server a little time to start
sleep 2

python3 test_node.py -host localhost -port 1884 node_desc_b.json -val -1 &
python3 test_node.py -host localhost -port 1884 node_desc_a.json -val 0 &
