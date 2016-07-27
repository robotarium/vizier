import asyncio
import mqtt_interface.mqttInterface as mqttInterface
import functools as ft
import time
import json
import argparse
import queue
import node.vizier_node_pure_asyncio as vizier_node

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

    # Ensure that information in our configuration file is accurate

    # I hate using the UDP...let's just ignore that for now...

    node = vizier_node.VizierNode(args.host, args.port, node_descriptor)
    node.start()
    setup_information = node.connect()

    print("SETUP INFO: " + repr(setup_information))

    #node.stop()

if(__name__ == "__main__"):
    main()
