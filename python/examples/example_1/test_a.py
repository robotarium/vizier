# Module: Paul Glotfelter
# Test Code: Ian Buckley
# 7/2/2018
# Description: This program increments an index value on each message recieved to print successive words in a string.

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
    publishable_link = list(node.publishable_links)[0]

    # This will be printed from
    paragraph = "The quick brown fox jumps over the lazy dog.".split(' ')

    # Keep Track of the Initial Conditions so that Output is Ordered
    val = 0
    node.publish(publishable_link, str(val))
    # Loop until the string is printed, then terminate
    while True:
        try:
            val = int(msg_queue.get(timeout=10).decode(encoding='UTF-8'))
        except Exception as e:
            print('Retrying after error {}'.format(repr(e)))
            node.publish(publishable_link, str(val))
            continue
        if (val >= len(paragraph)):
            node.publish(publishable_link, str(val+1))
            break
        print(paragraph[val])
        node.publish(publishable_link, str(val+1))
    node.stop()


if(__name__ == "__main__"):
    main()
