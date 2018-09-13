# Module: Paul Glotfelter
# Test Code: Ian Buckley
# 7/2/2018
# Description: This program prints the time recieved from the talker

import json
import argparse
import vizier.node as vizier_node


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
    node = vizier_node.Node(args.host, args.port, node_descriptor)
    node.start()

    # Get the links for Publishing/Subscribing
    subscribable_link = list(node.subscribable_links)[0]
    msg_queue = node.subscribe(subscribable_link)

    # Loop for 9ish seconds, then terminate
    val = 0
    while val <= 9:
        try:
            message = msg_queue.get(timeout=10).decode(encoding='UTF-8')
            val = float(message.replace(' ', '').split(':')[1])
            print(message)
        except KeyboardInterrupt:
            break
        except Exception:
            pass
    node.stop()


if(__name__ == "__main__"):
    main()
