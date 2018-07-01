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
import random
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
    subscribable_topic = node.get_subscribable_topics()[0]
    msg_queue = node.subscribe_with_queue(subscribable_topic)

    # Set the initial condition
    state = 10*random.random()-5
    dt = 0.0
    node.publish(publishable_topic,str(state))
    print('\n')
    while True:
        tick = time.time()
        try:
            message = msg_queue.get(timeout = 0.1).payload.decode(encoding='UTF-8')
            input = float(message)
        except KeyboardInterrupt:
            break
        except:
            input = 0.0
        state = state + dt * input
        print('State = {}'.format(state),end='\r')
        node.publish(publishable_topic,str(state))
        tock = time.time()
        dt = tock - tick
    print('\n')
    node.stop()

if(__name__ == "__main__"):
    main()
