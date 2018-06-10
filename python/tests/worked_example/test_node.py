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
    parser.add_argument("-val", type=int, help="The initial condition for this test", default=1)

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
    subscribable_topic = node.get_subscribable_topics()[0]
    msg_queue = node.subscribe_with_queue(subscribable_topic)
    publishable_topic = node.get_publishable_topics()[0]

    # This will be printed from
    paragraph="The quick brown fox jumps over the lazy dog.".split(' ')

    # Keep Track of the Initial Conditions so that Output is Ordered
    init_val = args.val
    val = init_val
    if init_val==0:
        node.publish(publishable_topic,str(val))

    # Loop until the string is printed, then terminate
    while True:
        try:
            val = int(msg_queue.get(timeout=10).payload.decode(encoding='UTF-8'))
        except Exception as e:
            if init_val==0:
                node.publish(publishable_topic,str(val))
            continue
        if (val>=len(paragraph)):
            node.publish(publishable_topic,str(val+1))
            break
        print(paragraph[val])
        node.publish(publishable_topic,str(val+1))

    node.stop()

if(__name__ == "__main__"):
    main()
