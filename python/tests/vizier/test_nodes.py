import time
import json
import argparse
import vizier.node as node


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("node_descriptor_a", help=".json file node information")
    parser.add_argument("node_descriptor_b", help='.json file node information')
    parser.add_argument("-port", type=int, help="MQTT Port", default=8080)
    parser.add_argument("-host", help="MQTT Host IP", default="localhost")

    args = parser.parse_args()

    # Ensure that we can open the nodes file
    node_descriptor = None

    try:
        f = open(args.node_descriptor_a, 'r')
        node_descriptor_a = json.load(f)
        f.close()
    except Exception as e:
        print(repr(e))
        print("Couldn't open given node file " + args.node_descriptor_a)
        return -1

    try:
        f = open(args.node_descriptor_b, 'r')
        node_descriptor_b = json.load(f)
        f.close()
    except Exception as e:
        print(repr(e))
        print("Couldn't open given node file " + args.node_descriptor_b)
        return -1

    node_a = node.Node(args.host, args.port, node_descriptor_a)
    node_a.start()

    node_b = node.Node(args.host, args.port, node_descriptor_b)
    node_b.start()

    print("Publishable topics:", node_b.publishable_links)
    print('Subscriptable topics:', node_b.subscribable_links)
    print("Gettable topics:", node_b.gettable_links)
    print('Offerable topics:', node_b.puttable_links)

    node_a.stop()
    node_b.stop()


if(__name__ == "__main__"):
    main()
