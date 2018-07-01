# Module: Paul Glotfelter
# Test Code: Ian Buckley
# 6/10/2018
# Description: This program increments an index value on each message recieved to print successive words in a string.

import asyncio
import functools as ft
import time
import json
import argparse
import queue
import node.vizier_node as vizier_node

def main():

    # Parse Command Line Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("node_descriptor", help=".json file node information")
    parser.add_argument("-port", type=int, help="MQTT Port", default=8080)
    parser.add_argument("-host", help="MQTT Host IP", default="localhost")

    args = parser.parse_args()

    # Ensure that Node Descriptor File can be Opened
    node_descriptor = None
    try:
        f = open(args.node_descriptor, 'r')
        node_descriptor = json.load(f)
        f.close()
    except Exception as e:
        print(repr(e))
        print("Couldn't open given node file " + args.node_descriptor)
        return -1

    # Start the Node
    node = vizier_node.VizierNode(args.host, args.port, node_descriptor)
    node.start()
    setup_information = node.connect()

    # Get the Topics for Publishing/Subscribing
    publishable_topic = node.get_publishable_topics()[0]

    tick = time.time()
    while time.time()-tick<=10:
        output = 'Seconds since start: ' + str(time.time()-tick)
        node.publish(publishable_topic,output)
    node.stop()

if(__name__ == "__main__"):
    main()
