import asyncio
import functools as ft
import time
import json
import argparse
import queue
import node.vizier_node as vizier_node

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("node_descriptor", help=".json file node information")
    parser.add_argument("-port", type=int, help="MQTT Port", default=8080)
    parser.add_argument("-host", help="MQTT Host IP", default="localhost")

    args = parser.parse_args()

    #Ensure that we can open the nodes file
    node_descriptor = None

    try:
        f = open(args.node_descriptor, 'r')
        node_descriptor = json.load(f)
        f.close()
    except Exception as e:
        print(repr(e))
        print("Couldn't open given node file " + args.node_descriptor)
        return -1

    node = vizier_node.VizierNode(args.host, args.port, node_descriptor)
    node.start()
    setup_information = node.connect()

    print("Publishable topics:", node.get_publishable_topics())
    print('Subscriptable topics:', node.get_subscribable_topics())

    node.stop()

if(__name__ == "__main__"):
    main()
