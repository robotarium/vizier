#!/bin/bash

# Give the server a little time to start
sleep 2

python3 test_node.py -host 192.168.1.2 -port 1884 node_desc_c.json & >/dev/null 2>&1

python3 test_node.py -host 192.168.1.2 -port 1884 node_desc_b.json & >/dev/null 2>&1

python3 test_node.py -host 192.168.1.2 -port 1884 node_desc_a.json & >/dev/null 2>&1
