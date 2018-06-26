import time
import json
import argparse
import vizier.node as node


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("node_descriptor", help=".json file node information")
    parser.add_argument("-port", type=int, help="MQTT Port", default=8080)
    parser.add_argument("-host", help="MQTT Host IP", default="localhost")

    args = parser.parse_args()

    # Ensure that we can open the nodes file
    node_descriptor = None

    try:
        f = open(args.node_descriptor, 'r')
        node_descriptor = json.load(f)
        f.close()
    except Exception as e:
        print(repr(e))
        print("Couldn't open given node file " + args.node_descriptor)
        return -1

    v_node = node.Node(args.host, args.port, node_descriptor)
    v_node.start()

    print("Publishable topics:", v_node.publishable_links)
    print('Subscriptable topics:', v_node.subscribable_links)
    print("Gettable topics:", v_node.gettable_links)
    print('Puttable topics:', v_node.puttable_links)

    # Sleep for a few seconds to allow setup and then stop the node
    time.sleep(10)
    v_node.stop()


if(__name__ == "__main__"):
    main()
