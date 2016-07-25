import edfg
import node
import asyncio
import mqttInterface
import pipeline
import functools as ft
import time
import json
import argparse
import queue
import collections
import pprint
from utils import *

#TODO: INCORPORATE SUB DEPENDENCY MATCHING INTO FRAMEWORK (PROBABLY SHOULD BE DONE @ GENERATION)
    #TODO: This all assumes that we've safely generated these files.  Should be fairly easy to do, actually!
    #TODO: I should write a little script to generate these files and check everything

#TODO: Check the prior node produces the correct dependencies.  Should be a fairly minor modification

#TODO: AFTER INITIALIZATION, WE CAN JUST MAKE CHANNELS THAT THE NODE'S DATA CAN BE REQUESTED FROM

@asyncio.coroutine
def send_and_wait_for_message_and_retry(mqtt_client, send_to, message, retries=15, timeout=5):

    current_retry = 0

    while True:
        try:
            yield from mqtt_client.send_message(send_to, bytearray(json.dumps(message).encode(encoding="UTF-8")))
            network_message = yield from mqtt_client.wait_for_message(message["response"]["link"], timeout)
            return network_message
        except Exception as e:
            print("retry")
            if(current_retry == retries):
                raise e
            current_retry += 1

@asyncio.coroutine
def decode_mqtt_to_json(message):
    #print("Got message: " + repr(message.payload))
    return json.loads(message.payload.decode(encoding="UTF-8"))

# Function "decorator" for creating the handshake
def create_node_descriptor_retriever_coroutine(mqtt_client, link, setup_channel):


    response_channel = setup_channel + '/' + link + '/response'

    vizier_get = create_node_descriptor_get(link, response_channel)

    print(link)

    node_descriptor_retriever = pipeline.construct(ft.partial(mqtt_client.subscribe, response_channel),
                                                   ft.partial(send_and_wait_for_message_and_retry, mqtt_client, link, vizier_get),
                                                   decode_mqtt_to_json)

    return node_descriptor_retriever

def create_put_response_coroutine(mqtt_client, to_link, message):
    response_channel = message["response"]["link"]

    print("waiting for message on : " + response_channel)
    print("sending message on : " + to_link)

    coroutine = pipeline.construct(ft.partial(mqtt_client.subscribe, response_channel),
                                   ft.partial(mqtt_client.send_message, to_link, json.dumps(message)),
                                   ft.partial(mqtt_client.wait_for_message, response_channel),
                                   decode_mqtt_to_json,
                                   cleanup=[ft.partial(mqtt_client.unsubscribe, response_channel)])

    return coroutine

def create_node_handshake(mqtt_client, link, requests):

    coroutines = []

    for request in requests:
        requested_link = request["link"]
        response_channel = request["response"]["link"]

        message = create_vizier_put(requested_link, response_link= "vizier/" + link + "/response")

        coroutines.append(create_put_response_coroutine(mqtt_client, response_channel, message))

        print("MESSAGE: " + repr(message))

    def f(*responses):

        # responses are the links that this node needs to start

        for coroutine in coroutines:
            try:
                result = mqtt_client.run_pipeline(coroutine())
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
    Constructs a network given the nodes
    """

    # First, we need to grab all the node descriptors
    ndrs = [create_node_descriptor_retriever_coroutine(mqtt_client, nd + "/node_descriptor", setup_channel) for nd in node_descriptors]
    pipe = pipeline.construct(ndrs)

    try:
        results = mqtt_client.run_pipeline(pipe())
    except queue.Empty as e:
        print("Didn't receive contact from all nodes")
        raise e

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

    # Ensure that all dependencies have been met
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
    #Don't need this for initialization
    app_builder = edfg.EDFGBuilder()

    for link in node_links:
        app_builder.with_node(node.Node(link, [x["link"] for x in node_links[link]], create_node_handshake(mqtt_client, link, node_links[link])))

    app_builder.with_node(node.Node("all_results", node_links.keys(), create_node_listener(node_links.keys())))

    app = app_builder.build()

    return(app.execute(pretty_print = True))

def execute():
    pass

def construct(host, port, setup_channel, *node_descriptors):

    # WE NOW HAVE THREE PHASES
    # CONSTRUCT -> COMPILE -> EXECUTE


    #TODO: I need to build all the dependencies up front to make it more obvious what's happening

    # Attempt to connect to MQTT broker
    try:
        mqtt_client = mqttInterface.MQTTInterface(port=port, host=host)
    except ConnectionRefusedError as e:
        print("Couldn't connect to MQTT broker at " + str(args.host) + ":" + str(args.port) + ". Exiting.")
        raise e

    # Start the MQTT interface
    mqtt_client.start()

    all_links = initialize(mqtt_client, setup_channel, *node_descriptors)
    print(all_links)
    result = assemble(mqtt_client, all_links)

    print(result)

def main():
    descriptor1 = \
    {
      "end_point" : "matlab_api",
      "links" :
      {
        "/job_runner" :
        {
          "requests" :
          [
            {
             "type" : "REQUEST",
             "link" : "vizier/overhead_tracker/all_robot_pose_data",
             "response" : {"type" : "RESPONSE", "link" : "/1"}
            }
          ],
          "links" : {}
        },
        "/18:fe:34:d9:a0:91" :
        {
          "requests" : [],
          "links" : {}
        },
      },
      "requests" : [],
      "version" : "0.2"
    }

    all_links = generate_links_from_descriptor(descriptor1)

    print(all_links)

    actual_links = {x for x in all_links.keys()}
    required_links = {y["link"] for x in all_links.values() for y in x}

    print(actual_links)
    print(required_links)

    if((actual_links & required_links) != actual_links):
        print("Deps not satisfied: " + repr((required_links - actual_links)))

if(__name__ == "__main__"):
    main()
