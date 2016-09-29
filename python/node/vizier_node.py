import asyncio
import mqtt_interface.mqttInterface as mqttInterface
import mqtt_interface.pipeline as pipeline
import functools as ft
import json
import queue
from utils.utils import *

#TODO: Add final intialization stage and heartbeat

def create_wait_for_message_and_retry_coroutine(mqtt_client, link, timeout=10, retries=5):
    """ A returns a co-routine that waits for a message on a particular topic and will retry with a given timeout. """

    @asyncio.coroutine
    def f():

        yield from mqtt_client.subscribe(link)

        current_retry = 0

        #  Try to handshake the messages
        while True:
            try:
                network_message = yield from mqtt_client.wait_for_message(link, timeout)
                message = json.loads(network_message.payload.decode(encoding="UTF-8"))
                break
            except Exception as e:
                print("retry")
                if(current_retry == retries):
                    raise e
                current_retry += 1

        return message

    return f

# QUICK CURRY FUNCTION FOR HANDLING GET REQUESTS ON CHANNELS
def _curry_get_message_handler(mqtt_client, info):
    """ Returns a message handler for use as a callback """

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
        """ Offers data on a particular link """
        self.links[link] = info;
        self.mqtt_client.subscribe_with_callback(link, _curry_get_message_handler(self.mqtt_client, info))

    def start(self):
        """ Start the MQTT client """
        self.mqtt_client.start()

    def stop(self):
        """ Stop the MQTT client """
        self.mqtt_client.stop()

    def connect(self):
        """ Connect to the main Vizier server """

        setup_channel = 'vizier/setup'

        loop = asyncio.get_event_loop()

        for requests in self.expanded_links.values():
            for request in requests:
                loop.run_until_complete(self.mqtt_client.subscribe(request["response"]["link"]))

        #Subscribe to response channels, then offer up our node descriptor so that the server can grab it

        self.offer(self.end_point + '/node_descriptor', self.node_descriptor)
        print("OFFERING NODE DESCRIPTOR ON:" + self.end_point + '/node_descriptor')

        #Get final setup information from the server
        requested_links = [x["link"] for y in self.expanded_links.values() for x in y ]
        coroutines = [create_wait_for_message_and_retry_coroutine(self.mqtt_client, x["response"]["link"])() for y in self.expanded_links.values() for x in y ]

        # Execute the constructed asyncio program
        result = loop.run_until_complete(asyncio.gather(*coroutines))

        # Pull out the setup information from the sent message
        setup_information = {x : y["body"] for x, y in zip(requested_links, result)}

        return setup_information
