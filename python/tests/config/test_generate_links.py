import asyncio
import functools as ft
import time
import json
import argparse
import queue
import node.vizier_node as vizier_node
from utils.utils import *

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("node_descriptor", help=".json file node information")

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

    print(generate_links_from_descriptor(node_descriptor))

if(__name__ == "__main__"):
    main()
