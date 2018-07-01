import asyncio
import functools as ft
import time
import json
import argparse
import queue
import vizier.vizier as vizier

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("config", help=".json file node information")
    parser.add_argument("-port", type=int, help="MQTT Port", default=1884)
    parser.add_argument("-host", help="MQTT Host IP", default="localhost")

    args = parser.parse_args()

    print(args)

    #Ensure that we can open the nodes file
    config = None
    try:
        f = open(args.config, 'r')
        config = json.load(f)
        f.close()
    except Exception as e:
        print(repr(e))
        print("Couldn't open given node file " + args.config)
        return -1

    setup_channel = 'vizier/setup'
    node_descriptors = config["nodes"]

    v = vizier.Vizier(args.host, args.port)
    v.start(setup_channel, node_descriptors)
    
    input('Press something to quit')

    v.stop()

if(__name__ == "__main__"):
    main()
