import asyncio
import mqtt_interface.mqttinterface as mqtt
import pprint
import functools as ft
import time
import json
import argparse
import queue
import collections
from utils.utils import *

import concurrent.futures as futures

### For logging ###
import logging
import logging.handlers as handlers

### LOGGING ###

logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Vizier():

    def __init__(self, host, port):
        logger.info(port)
        self.mqtt_client = mqtt.MQTTInterface(host=host, port=port)
        self.executor = futures.ThreadPoolExecutor(max_workers=500)

    def _node_descriptor_retriever(self, setup_channel, link, timeout=5, retries=5):
        """
        This function creates an asyncio that handles the retrieval of node descriptors from various corresponding nodes.
        timeout: how long the function waits for a response per retry; so the total time takes timeout * retries.
        retries: how many times to retry waiting for a node descriptor.  In practice, I've actually never had to retry.
        """

        response_channel = setup_channel + '/' + link + '/response'
        vizier_get = create_vizier_get_message(link, response_channel)

        # Subscribe to the response channel
        _, q = self.mqtt_client.subscribe(response_channel)

        message = None

        # Do this for number of retries
        for i in range(retries):
            try:
                # Send a message to the node descriptor channel, which should illicit a response
                self.mqtt_client.send_message(link, json.dumps(vizier_get).encode(encoding='UTF-8'))
                # Try to get a message on the response channel
                mqtt_message = q.get(timeout=timeout)
                # Load/decode the message from byte array -> python
                message = json.loads(mqtt_message.payload.decode(encoding='UTF-8'))
                message = message["body"]
                break
            except Exception as e:
                logger.warning(repr(e))
                logger.warning("Retrying node descriptor retrieval for node: " + link)

        # When we're done, unsubscribe from the response channel
        self.mqtt_client.unsubscribe(response_channel)

        return message

    def _initialize(self, setup_channel, *node_descriptors):
        """
        Constructs a network given the nodes in node descriptors.  This function depends
        on each node offering its node descriptor properly.  Each node descriptor should
        always be offered.
        """

        # Retrieve all the node descriptor from the supplied ones that we're expecting
        node_descriptor_links = [nd + "/node_descriptor" for nd in node_descriptors]
        # Call this function on all the links to attempts to retrieve a bunch of node descriptors
        # concurrently
        results = list(self.executor.map(ft.partial(self._node_descriptor_retriever, setup_channel), node_descriptor_links))

        for i, r in enumerate(results):
            if r is None:
                logger.error("Couldn't retrieve descriptor for node: %s", repr(node_descriptor_links[i]))
                return None

        logger.info("Resulting link structure is: %s", repr(results))

        # Do some error checking to make sure we got the right nodes

        received_nodes = [node["end_point"] for node in results]

        compare = lambda x, y: collections.Counter(x) == collections.Counter(y)

        if not set(received_nodes) == set(node_descriptors):
            logger.error("Received nodes (%s) not the same as supplied node descriptors (%s)", repr(received_nodes), repr(node_descriptors))

        #Build dependencies.  Basically, just pull everything into a dictionary for easy access
        node_descriptors_ret = {node["end_point"] : node for node in results}

        all_links = {}
        for descriptor in results:
            node_links = generate_links_from_descriptor(descriptor)
            old_length = len(all_links)
            all_links.update(node_links)

            if((len(node_links) + old_length) != len(all_links)):
                # print("Duplicate links!")
                logger.error("Received duplicate links")
                raise ValueError

        actual_links = {x for x in all_links.keys()}
        required_links = {y for x in all_links.values() for y in x['requests']}

        if((actual_links & required_links) != required_links):
            p_printer = pprint.PrettyPrinter(indent=4)
            p_printer.pprint("The following dependencies were not satisfied: " + repr((required_links - actual_links)))
            p_printer.pprint(all_links)

            raise ValueError

        return {descriptor['end_point'] : generate_links_from_descriptor(descriptor) for descriptor in results}

    def _assemble(self, node_links):
        """
        Assemble assembles the vizier network, connecting all the required nodes to their specified dependencies.  In particular,
        it handshakes all the nodes with their dependencies to make sure that they're properly registered on the network.
        """

        # Could do something more clever here eventually
        link_mapping = {x : {'link': x, 'type': node_links[y][x]['type']} for y in node_links for x in node_links[y]}

        # Offer something on each topic.
        for x in link_mapping:
            self.offer('vizier/' + x, link_mapping[x])

        return True

    def offer(self, link, data):

        def f(network_message):

            try:
                network_message = json.loads(network_message.payload.decode(encoding="UTF-8"))
            except Exception as e:
                # TODO: (PAUL) Change the logger to be passed into function
                logger = logging.getLogger(__name__)
                logger.error("Got malformed network message")

            if('response' in network_message and 'link' in network_message['response']):
                response_channel = network_message['response']['link']
                message = create_vizier_get_response(data, message_type='data')
                self.mqtt_client.send_message(response_channel, json.dumps(message).encode(encoding='UTF-8'))

        self.mqtt_client.subscribe_with_callback(link, f)

    def start(self, setup_channel, node_descriptors):
        # Start the MQTT client
        self.mqtt_client.start()
        node_links = self._initialize(setup_channel, *node_descriptors)
        result = None
        if(node_links is not None):
            result = self._assemble(node_links)

        return result

    def stop(self):
        self.mqtt_client.stop()
