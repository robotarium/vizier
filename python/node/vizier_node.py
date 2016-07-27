import asyncio
import mqtt_interface.mqttInterface as mqttInterface
import pipeline
import functools as ft
import time
import json
import argparse
import queue

#def handle source nodes

@asyncio.coroutine
def subscribe_and_return(client, channel):
    yield from client.subscribe(channel)
    return channel

@asyncio.coroutine
def send_and_return(mqtt_client, channel, message):
    yield from mqtt_client.send_message(channel, message)
    return channel

@asyncio.coroutine
def parse_setup_message(message):
    decoded_message = json.loads(message.payload.decode(encoding="UTF-8"))
    return decoded_message

@asyncio.coroutine
def send_ok_message(mqtt_client, message):
    decoded_message = json.loads(message.payload.decode(encoding="UTF-8"))
    response_channel = decoded_message["response"]
    #print("Received setup message: " + repr(decoded_message))
    yield from mqtt_client.send_message(response_channel, json.dumps({"status" : "running", "error" : "none"}))
    return decoded_message

def connect(host, port, node_descriptor):

    try:
        mqtt_client = mqtt_interface.MQTTInterface(port=port, host=host)
    except ConnectionRefusedError as e:
        print("Couldn't connect to MQTT broker at " + str(args.host) + ":" + str(args.port) + ". Exiting.")
        return -1

    # Start the MQTT interface
    mqtt_client.start()

    # First, we need to grab all the node descriptors

    # DON'T FORGET TO UNSUBSCRIBE FROM CHANNELS
    setup_channel = node_descriptor["meta"]["mqtt_info"]["setup_channel"] + "/" + node_descriptor["attributes"]["attribute"]
    response_channel = node_descriptor["response"]

    #print("Sending on : " + setup_channel)
    #print("Listening on: " + response_channel)

    pipe = pipeline.construct(mqtt_client.subscribe,
                              ft.partial(mqtt_client.send_message, setup_channel, json.dumps(node_descriptor)),
                              ft.partial(mqtt_client.wait_for_message, response_channel),
                              ft.partial(send_ok_message, mqtt_client))

    setup_info = mqtt_client.run_pipeline(pipe(response_channel))

    mqtt_client.stop()

    return setup_info


def main():

    parser = argparse.ArgumentParser()
    #TODO: Add support for separate experiments
    #parser.add_argument("experimend_id", type=int, help="The ID of the experiment")
    parser.add_argument("node_descriptor", help=".json file node information")
    parser.add_argument("-port", type=int, help="MQTT Port", default=8080)
    parser.add_argument("-host", help="MQTT Host IP", default="localhost")

    args = parser.parse_args()

    print(args)

    #Ensure that we can open the nodes file

    node_descriptors = None
    try:
        f = open(args.node_descriptor, 'r')
        node_descriptors = json.load(f)
        f.close()
    except Exception as e:
        print(repr(e))
        print("Couldn't open given node file " + args.node_descriptor)
        return -1

    # Ensure that information in our configuration file is accurate

    # I hate using the UDP...let's just ignore that for now...

    # Attempt to connect to MQTT broker

if(__name__ == "__main__"):
    main()
