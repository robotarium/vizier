import asyncio
import mqttInterface
import pipeline
import functools as ft
import json
import queue
from utils import *

### COROUTINES FOR SENDING/RECEIVING MESSAGE ###

#DEFAULT CHANNELS ARE...
#Node descriptor (node level) /node/node_descriptor

def create_pipeline_print(to_print, end="\n"):
    @asyncio.coroutine
    def f(*arg):
        print(to_print, end = end)
        if(len(arg) != 0):
            return arg[0]

    return f

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
    yield from mqtt_client.send_message(response_channel, json.dumps({"status" : "running", "error" : "none"}))
    return decoded_message

@asyncio.coroutine
def wait_and_send(mqtt_client, channel):
    message = yield from mqtt_client.wait_for_message(channel)
    message = json.loads(message.payload.decode(encoding="UTF-8"))
    return message

def ignore_after_args(func, count):
    @ft.wraps(func)
    def newfunc(*args, **kwargs):
        return func(*(args[:count]), **kwargs)
    return newfunc

### QUICK CURRY FUNCTION FOR HANDLING GET REQUESTS ON CHANNELS

def _curry_get_message_handler(mqtt_client, info):

    def f(network_message):
        try:
            network_message = json.loads(network_message.payload.decode(encoding="UTF-8"))
        except Exception as e:
            print("Got malformed network message")

        response_channel = network_message["response"]["link"]
        mqtt_client.send_message2(response_channel, json.dumps(info))

    return f

class VizierNode:

    def __init__(self, broker_host, broker_port, node_descriptor):
        self.mqtt_client = mqttInterface.MQTTInterface(port=broker_port, host=broker_host)
        self.node_descriptor = node_descriptor
        self.end_point = node_descriptor["end_point"]
        self.expanded_links = generate_links_from_descriptor(node_descriptor)
        self.links = {}
        self.host = broker_host
        self.port = broker_port

    def offer(self, link, info):
        self.links[link] = info;
        self.mqtt_client.subscribe_with_callback(link, _curry_get_message_handler(self.mqtt_client, info))

    def start(self):
        self.mqtt_client.start()
        self.offer(self.end_point + '/node_descriptor', self.node_descriptor)

    def stop(self):
        self.mqtt_client.stop()

    def connect(self):

        setup_channel = 'vizier/setup'

        wait_and_sends = []
        subscribes = []

        for requests in self.expanded_links.values():
            for request in requests:
                print(request["response"]["link"])
                f = ft.partial(wait_and_send, self.mqtt_client, request["response"]["link"])
                f = ignore_after_args(f, 0)
                wait_and_sends.append(f)
                subscribes.append(ft.partial(self.mqtt_client.subscribe, request["response"]["link"]))

        pipe = pipeline.construct(subscribes,
                                  create_pipeline_print("Subscribing to setup channel: " + repr(self.expanded_links) +  " " + bcolors.OKGREEN + "[ ok ]" + bcolors.WHITE),
                                  wait_and_sends,
                                  create_pipeline_print(bcolors.OKGREEN + "[ ok ]" + bcolors.WHITE),
                                  create_pipeline_print("Sending OK message:", end = " "),
                                  create_pipeline_print(bcolors.OKGREEN + "[ ok ]" + bcolors.WHITE))

        setup_information = None

        try:
            setup_information = self.mqtt_client.run_pipeline(pipe())
        except Exception as e:
            print("Couldn't complete vizier handshake for node: " + node_descriptor["attributes"]["attribute"])

        return setup_information

if(__name__ == "__main__"):
    main()
