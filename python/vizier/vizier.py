import vizier.edfg as edfg
import vizier.node as node
import vizier.colors as colors
import asyncio
import mqtt_interface.mqttInterface as mqttInterface
import mqtt_interface.pipeline as pipeline
import functools as ft
import time
import json
import argparse
import queue
import collections
import pprint
from utils.utils import *

#TODO: Check the prior node produces the correct dependencies.  Should be a fairly minor modification

#TODO: AFTER INITIALIZATION, WE CAN JUST MAKE CHANNELS THAT THE NODE'S DATA CAN BE REQUESTED FROM

def create_get_response_coroutine(mqtt_client, to_link, message):
    """
    This function creates and returns an asyncio coroutine that creates a JSON message and
    sends it to the received vizier URI link.
    """
    @asyncio.coroutine
    def f():

        # In a put, I can give a link back.  The client should then follow the link with another get

        try:
            json_message = json.dumps(message)
        except Exception as e:
            raise

        yield from mqtt_client.send_message(to_link, json_message)

    return f

def create_node_descriptor_retriever_coroutine(mqtt_client, setup_channel, link, timeout = 5, retries = 5):
    """
    This function creates an asyncio that handles the retrieval of node descriptors from various corresponding nodes.
    timeout: how long the function waits for a response per retry; so the total time takes timeout * retries.
    retries: how many times to retry waiting for a node descriptor.  In practice, I've actually never had to retry.
    """
    response_channel = setup_channel + '/' + link + '/response'
    vizier_get = create_vizier_get_message(link, response_channel)

    @asyncio.coroutine
    def f():

        yield from mqtt_client.subscribe(response_channel)

        current_retry = 0

        #  Try to handshake the messages
        while True:
            try:
                yield from mqtt_client.send_message(link, bytearray(json.dumps(vizier_get).encode(encoding="UTF-8")))
                network_message = yield from mqtt_client.wait_for_message(response_channel, timeout)
                message = json.loads(network_message.payload.decode(encoding="UTF-8"))
                break
            except Exception as e:
                print("Retrying node descriptor retrieval for node: " + link)
                if(current_retry == retries):
                    raise e
                current_retry += 1

        return message

    return f

def create_node_handshake(mqtt_client, link, requests):
    """
    This function creates and returns a function for use in the EDFG that handles handshaking nodes after startup.
    That is, repeatedly calls 'create_get_response_coroutine' to realize all the needed handshakes.
    """
    coroutines = []

    for request in requests:
        requested_link = request["link"]
        response_channel = request["response"]["link"]

        message = create_vizier_get_response(requested_link, message_type="data")

        coroutines.append(create_get_response_coroutine(mqtt_client, response_channel, message))

        print("MESSAGE: " + repr(message))
        print("ON CHANNEL: " + repr(response_channel))

    loop = asyncio.get_event_loop()

    # Responses are responses from the dependencies for this node.
    def f(*responses):
        for coroutine in coroutines:
            try:
                result = loop.run_until_complete(coroutine())
            except queue.Empty as e:
                # If we don't get a response from the node, "throw" an error
                print("DIDN'T GET A RESPONSE")
                #result = {"status" : "error", "error" : "Couldn't initialize node: " + node_id}

        return link

    return f

def create_node_listener(received_nodes):

    def f(*args):
        return dict(zip(received_nodes, args))

    return f

def initialize(mqtt_client, setup_channel, *node_descriptors):
    """
    Constructs a network given the nodes in node descriptors.
    """

    # Retrieve all the node descriptor from the supplied ones that we're expecting
    node_descriptor_links = [nd + "/node_descriptor" for nd in node_descriptors]

    results = []
    for ndl in node_descriptor_links:
        ndrc = create_node_descriptor_retriever_coroutine(mqtt_client, setup_channel, ndl)
        try:
            result = asyncio.get_event_loop().run_until_complete(ndrc())
            print("GOT RESULT: " + ndl)
            results.append(result)
        except Exception as e:
            print("Didn't receive contact from all nodes")
            raise

    print("result: " + repr(results))

    # Do some error checking to make sure we got the right nodes

    received_nodes = [node["end_point"] for node in results]

    compare = lambda x, y: collections.Counter(x) == collections.Counter(y)

    if not set(received_nodes) == set(node_descriptors):
        print("Recieved nodes not the same as supplied node descriptors")
        print("Received nodes: " + repr(received_nodes))
        print("Supplied nodes: " + repr(node_descriptors))

    #Build dependencies.  Basically, just pull everything into a dictionary for easy access
    node_descriptors_ret = {node["end_point"] : node for node in results}

    all_links = {}
    for descriptor in results:
        node_links = generate_links_from_descriptor(descriptor)
        old_length = len(all_links)
        all_links.update(node_links)

        if((len(node_links) + old_length) != len(all_links)):
            print("Duplicate links!")
            raise ValueError

    print(all_links)

    # Ensure that all dependencies have been met.  This is just error checking
    # make sure that all the dependencies are satisfied.
    actual_links = {x for x in all_links.keys()}
    required_links = {y["link"] for x in all_links.values() for y in x}

    print(actual_links)
    print(required_links)

    print(actual_links & required_links)

    if((actual_links & required_links) != required_links):
        print("The following dependencies were not satisfied: " + repr((required_links - actual_links)))
        raise ValueError

    return all_links

def assemble(mqtt_client, node_links):
    """
    Assemble assembles the vizier network, connecting all the required nodes to their specified dependencies.  In particular,
    it handshakes all the nodes with their dependencies to make sure that they're properly registered on the network.
    """
    edfg_builder = edfg.EDFGBuilder()

    for link in node_links:
        edfg_builder.with_node(node.Node(link, [x["link"] for x in node_links[link]], create_node_handshake(mqtt_client, link, node_links[link])))

    # This is an extra node to print out all the nodes that have been received.
    edfg_builder.with_node(node.Node("all_results", node_links.keys(), create_node_listener(node_links.keys())))

    # This builds the
    edfg_ = edfg_builder.build()

    # Return the result of executing the EDFG with pretty printing for debugging
    return(edfg_.execute(pretty_print = True))

def execute():
    pass

def construct(host, port, setup_channel, *node_descriptors):

    #TODO: I need to build all the dependencies up front to make it more obvious what's happening

    # Attempt to connect to MQTT broker
    try:
        mqtt_client = mqttInterface.MQTTInterface(port=port, host=host)
    except ConnectionRefusedError as e:
        print("Couldn't connect to MQTT broker at " + str(args.host) + ":" + str(args.port) + ". Exiting.")
        raise e

    # Start the MQTT interface
    mqtt_client.start()

    # PErform the initialize -> assemble -> TODO: contstruct steps
    all_links = initialize(mqtt_client, setup_channel, *node_descriptors)
    print("ALL LINKS: " + repr(all_links))
    result = assemble(mqtt_client, all_links)

    print(result)

def main():
    pass

if(__name__ == "__main__"):
    main()
