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

#TODO: INCORPORATE SUB DEPENDENCY MATCHING INTO FRAMEWORK (PROBABLY SHOULD BE DONE @ GENERATION)
    #TODO: This all assumes that we've safely generated these files.  Should be fairly easy to do, actually!
    #TODO: I should write a little script to generate these files and check everything

#TODO: Check the prior node produces the correct dependencies.  Should be a fairly minor modification

#TODO: Try making a protocol for this

#VIZIER OFFERS UNIQUE CHANNEL GENERATOR ON /VIZIER/END_POINT/UNIQUE_RESPONSE, SENDS BACK ON /END_POINT/UNIQUE_

def network_dep_topic(node_id):
    return "/nodes/" + node_id

def extended_network_dep_topic(node_id, sub_dep):
    return network_dep_topic(node_id) + "/" + sub_dep

def expand_network_dependencies(node_descriptor, light = False):

    if(light):

        network_deps = []

        for x in node_descriptor["deps"]:

            network_descriptor = [network_dep_topic(x["attribute"])]

            for y in x["sub_attributes"]:
                network_descriptor.append(extended_network_dep_topic(x["attribute"], y))

            network_deps = network_deps + network_descriptor

        return network_deps
    else:

        network_deps = {}

        for x in node_descriptor["deps"]:

            network_descriptor = {"network_attribute" : network_dep_topic(x["attribute"]),
            "network_sub_attributes" : {}}

            for y in x["sub_attributes"]:
                network_descriptor["network_sub_attributes"][y] = extended_network_dep_topic(x["attribute"], y)

            network_deps.update({x["attribute"] : network_descriptor})

        return network_deps

def expand_network_sub_attributes(node_descriptor, light=False):

    node_id = node_descriptor["attributes"]["attribute"]

    if(light):
        return [extended_network_dep_topic(node_id, x) for x in node_descriptor["attributes"]["sub_attributes"]]
    else:
        return {x : extended_network_dep_topic(node_id, x) for x in node_descriptor["attributes"]["sub_attributes"]}

@asyncio.coroutine
def decode_mqtt_to_json(message):
    #print("Got message: " + repr(message.payload))
    return json.loads(message.payload.decode(encoding="UTF-8"))

# Function "decorator" for creating the handshake
def create_node_descriptor_retriever_coroutine(mqtt_client, setup_channel, node_id):

    channel = setup_channel + "/" + node_id
    node_descriptor_retriever = pipeline.construct(ft.partial(mqtt_client.subscribe, channel),
    ft.partial(mqtt_client.wait_for_message, channel),
    decode_mqtt_to_json,
    cleanup=[ft.partial(mqtt_client.unsubscribe, channel)])

    return node_descriptor_retriever

# From my network dependencies, I will get
def create_node_handshake_coroutine(mqtt_client, node_response_channel, response_channel, network_attribute, network_sub_attributes, network_deps, light = False):

    #Construct setup information for node

    if(light):
        setup_info = {"deps" : network_deps , "attributes" : [network_attribute] + network_sub_attributes, "response" : response_channel}
    else:
        setup_info = {"deps" : network_deps,
        "network_attributes" : {"network_attribute" : network_attribute, "network_sub_attributes" : network_sub_attributes},
        "response" : response_channel}

    print("Setup info: " + repr(setup_info) + " is size " + repr(len(json.dumps(setup_info))))

    print("node_response_channel: " + node_response_channel)

    node_handshake = pipeline.construct(ft.partial(mqtt_client.subscribe, response_channel),
    ft.partial(mqtt_client.send_message, node_response_channel, json.dumps(setup_info)),
    ft.partial(mqtt_client.wait_for_message, response_channel),
    decode_mqtt_to_json,
    cleanup=[ft.partial(mqtt_client.unsubscribe, response_channel)])

    return node_handshake

def create_node_handshake(mqtt_client, node_descriptor):

    node_id = node_descriptor["attributes"]["attribute"]

    light = False

    if("meta" in node_descriptor):
        if("weight" in node_descriptor["meta"]):
            light = node_descriptor["meta"]["weight"] == "light"
        else:
            light = False
    else:
        light = False

    node_response_channel = node_descriptor["response"]
    network_attribute = network_dep_topic(node_id)
    response_channel = extended_network_dep_topic(node_id, "heartbeat")
    network_sub_attributes = expand_network_sub_attributes(node_descriptor, light = light)
    network_deps = expand_network_dependencies(node_descriptor, light = light)

    def f(*responses):

        errors = []

        for r in responses:
            if r["status"] != "running":
                print("There was an error initializing node: " + node_id + " with error: " + repr(r["error"]))
                errors.append(r["error"])

        if(len(errors) != 0):
            errors.append("Couldn't initialize node: " + node_id + " due to previous error.")
            return {"status" : "error", "error" : errors}
        else:
            result = None
            try:
                result = mqtt_client.run_pipeline(create_node_handshake_coroutine(mqtt_client, node_response_channel, response_channel, network_attribute, network_sub_attributes, network_deps, light = light)())
            except queue.Empty as e:
                # If we don't get a response from the node, "throw" an error
                result = {"status" : "error", "error" : "Couldn't initialize node: " + node_id}

            return result

    return f

def create_node_listener(received_nodes):

    def f(*args):
        return dict(zip(received_nodes, args))

    return f

def construct(host, port, setup_channel, *node_descriptors):
    """
    Constructs a network given the nodes
    """

    #TODO: I need to build all the dependencies up front to make it more obvious what's happening

    # Attempt to connect to MQTT broker
    try:
        mqtt_client = mqttInterface.MQTTInterface(port=port, host=host)
    except ConnectionRefusedError as e:
        print("Couldn't connect to MQTT broker at " + str(args.host) + ":" + str(args.port) + ". Exiting.")
        raise e

    # Start the MQTT interface
    mqtt_client.start()

    # First, we need to grab all the node descriptors
    ndrs = [create_node_descriptor_retriever_coroutine(mqtt_client, setup_channel, nd) for nd in node_descriptors]
    pipe = pipeline.construct(ndrs)



    try:
        results = mqtt_client.run_pipeline(pipe())
    except queue.Empty as e:
        print("Didn't receive any contact from all nodes")
        raise e

    received_nodes = [node["attributes"]["attribute"] for node in results]

    app_builder = edfg.EDFGBuilder()

    for nd in results:

        # Get the high-level node names
        high_level_deps = [x["attribute"] for x in nd["deps"]]

        app_builder.with_node(node.Node(nd["attributes"]["attribute"], high_level_deps, create_node_handshake(mqtt_client, nd)))

    app_builder.with_node(node.Node("all_results", received_nodes, create_node_listener(received_nodes)))

    app = app_builder.build()

    print(app.execute(pretty_print = True))

    mqtt_client.stop()

if(__name__ == "__main__"):
    main()
