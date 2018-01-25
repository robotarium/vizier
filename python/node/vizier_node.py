import asyncio
import mqtt_interface.mqttinterface as mqtt
import concurrent.futures as futures
import functools as ft
import json
import queue
from utils.utils import *

### For logging ###
import logging
import logging.handlers as handlers

logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

#TODO: Should change the whole 'offer' thing to be PUT/POST-like from HTML.  would
# actually make sense then.

class VizierNode:

    def __init__(self, broker_host, broker_port, node_descriptor, logging_config = None):
        self.mqtt_client = mqtt.MQTTInterface(port=broker_port, host=broker_host)
        self.node_descriptor = node_descriptor
        self.end_point = node_descriptor["end_point"]
        self.expanded_links = generate_links_from_descriptor(node_descriptor)
        self.links = {}
        self.host = broker_host
        self.port = broker_port

        # Defines the four kinds of data that we can expect.
        self.publishable_mapping = {}
        self.gettable_mapping = {}
        self.puttable_mapping = {}
        self.puttable_data = {}

        # Define an executor for making requests
        self.executor = None

        # Some logging definitions.  TODO: This probably needs to change at some point.
        if(logging_config):
            logging.configDict(logging_config)
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.DEBUG)

    # QUICK CURRY FUNCTION FOR HANDLING GET REQUESTS ON CHANNELS
    def _create_get_handler(self, info):
        """ Returns a message handler for use as a callback.  Used to
        respond to GET requests on particular topics."""
        def f(network_message):

            try:
                network_message = json.loads(network_message.payload.decode(encoding='UTF-8'))
            except Exception as e:
                # TODO: (PAUL) Change the logger to be passed into function
                logger = logging.getLogger(__name__)
                logger.error("Got malformed network message")

            # Make sure that we actually got a valid get request
            if('response' in network_message and 'link' in network_message['response']):
                response_channel = network_message['response']['link']
                json_message = create_vizier_get_response(info, message_type="data")
                self.mqtt_client.send_message(response_channel, json.dumps(json_message).encode(encoding='UTF-8'))

        return f

    def _get_request(self, link, retries=30, timeout=1):
        """Makes a get request for data on a particular topic.
        Will try 'retries' amount for 'timeout' seconds."""
        response_link = self.end_point + '/' + link + '/' + 'response'
        _, q = self.mqtt_client.subscribe(response_link)
        decoded_message = None
        get_request = json.dumps(create_vizier_get_message(link, response_link)).encode(encoding='UTF-8')

        for _ in range(retries):

            self.mqtt_client.send_message(link, get_request)

            # We expect this to potentially fail with a timeout
            try:
                mqtt_message = q.get(timeout=timeout)
            except Exception as e:
                # Don't try to decode message if we didn't get anything
                continue

            # Try to decode packet.  Could potentially fail
            try:
                decoded_message = json.loads(mqtt_message.payload.decode(encoding='UTF-8'))
                break
            except Exception as e:
                # Just pass here because we expect to fail.  In the future,
                # split the exceptions up into reasonable cases.
                pass

        if(decoded_message is None):
            self.logger.error('Get request on topic (%s) failed', link)

        # Make sure that we unsubscribe from the response channel
        self.mqtt_client.unsubscribe(response_link)
        return decoded_message

    def put(self, link, info):
        """ Offers data on a particular link """
        self.logger.info("OFFERING SOMETHING ON: " + link)

        self.links[link] = info;
        self.mqtt_client.subscribe_with_callback(link, self._create_get_handler(info))

    def start(self, timeout=5, retries=5):
        """ Start the MQTT client """
        self.mqtt_client.start()
        self.executor = futures.ThreadPoolExecutor(max_workers=100)

    def stop(self):
        """ Stop the MQTT client """
        self.mqtt_client.stop()

    def connect(self, timeout=5, retries=5):
        """ Connect to the main Vizier server """
        # TODO: Don't let this name be hard coded
        setup_channel = 'vizier/setup'

        print(self.expanded_links)

        #Subscribe to response channels, then offer up our node descriptor so that the server can grab it
        self.put(self.end_point + '/node_descriptor', self.node_descriptor)

        #Get final setup information from the server
        requested_links = [x for y in self.expanded_links.values() for x in y['requests']]
        proposed_links = self.expanded_links.keys()

        # Get final publish and receive names from server
        publish_results = list(self.executor.map(lambda x: self._get_request('vizier/' + x), proposed_links))
        receive_results = list(self.executor.map(lambda x: self._get_request('vizier/' + x), requested_links))

        if(None in publish_results):
            self.logger.error("Couldn't get all publish requests")
            return False

        if(None in receive_results):
            self.logger.error("Couldn't get all receive requests")
            return False

        # We need to account for subscribing, publishing, and getting
        providing_mapping = dict(zip(proposed_links, [x['body'] for x in publish_results]))
        receiving_mapping = dict(zip(requested_links, [x['body'] for x in receive_results]))

        puttable_topics = list(filter(lambda x: providing_mapping[x]['type'] == "DATA", providing_mapping))
        publishable_topics = list(filter(lambda x: providing_mapping[x]['type'] == "STREAM", providing_mapping))

        subscriptable_topics = list(filter(lambda x: receiving_mapping[x]['type'] == "STREAM", receiving_mapping))
        gettable_topics = list(filter(lambda x: receiving_mapping[x]['type'] == "DATA", receiving_mapping))

        self.logger.info('puttable topics: ' + repr(list(puttable_topics)))
        self.logger.info('Publishable topic: ' + repr(list(publishable_topics)))
        self.logger.info('Subscriptable topics: ' + repr(list(subscriptable_topics)))
        self.logger.info('Gettable topics: ' + repr(list(gettable_topics)))

        puttable_mapping = {x : providing_mapping[x]['link'] for x in puttable_topics}
        self.puttable_data = {x : {} for x in puttable_topics}

        # Ensure that we can offer data on the topics we said we would
        for x in puttable_topics:

            def f(network_message):
                try:
                    network_message = json.loads(network_message.payload.decode(encoding='UTF-8'))
                except Exception as e:
                    # TODO: (PAUL) Change the logger to be passed into function
                    logger = logging.getLogger(__name__)
                    logger.error("Got malformed network message")

                # Make sure that we actually got a valid get request
                if('response' in network_message and 'link' in network_message['response']):
                    response_channel = network_message['response']['link']
                    json_message = create_vizier_get_response(self.puttable_data[x], message_type="data")
                    self.mqtt_client.send_message(response_channel, json.dumps(json_message).encode(encoding='UTF-8'))

            # Offer data on a particular channel
            self.mqtt_client.subscribe_with_callback(x, f)

        self.publishable_mapping = {x : providing_mapping[x]['link'] for x in publishable_topics}
        self.subscribable_mapping = {x : receiving_mapping[x]['link'] for x in subscriptable_topics}
        self.gettable_mapping = {x : receiving_mapping[x]['link'] for x in gettable_topics}

        # Handle gettable topics

        self.logger.info('Successfully connected to Vizier network')

        return True

    def publish(self, topic, data):

        if(topic in self.publishable_mapping):
            actual_topic = self.publishable_mapping[topic]
            self.mqtt_client.send_message(actual_topic, data)
        else:
            self.logger.error('Requested topic (%s) not in publish mapping.', topic)

    def post(self, topic, data):
        """ Kind of like HTML POST.  Updates the data available
        on a particular DATA topic."""
        # Check to ensure that we can post on this topic
        if(topic in self.puttable_data):
            # Set the data that we're currently offering
            self.puttable_data[topic] = data
        else:
            self.logger.error('Requested topic (%s) not in puttable topics.', topic)

    def get_data(self, topic, retries=1, timeout=5):
        message = self._get_request(topic, retries=retries, timeout=timeout)
        return message["body"]

    def subscribe_with_queue(self, topic):
        """Subscribes to a particular topic and returns a queue
        containing the data."""
        q = None
        if(topic in self.subscribable_mapping):
            actual_topic = self.subscribable_mapping[topic]
            _, q = self.mqtt_client.subscribe()
        else:
            self.logger.error('Requested topic (%s) not in received topics')

        return q

    def subscribe_with_callback(self, topic, callback):
        """Subscribes to a particular topic, passing each received message to
        the callback"""
        if(topic in self.subscribable_mapping):
            actual_topic = self.subscribable_mapping[topic]
            self.mqtt_client.subscribe_with_callback(topic, callback)
        else:
            self.logger.error('Requested topic (%s) not in received topics')

    def unsubscribe(self, topic):
        """
        Unsubscrbes from a particular topic.  This will remove any effects from
        offering, subscribing with a callback, or subscribing with a queue.
        """
        self.mqtt_client.unsubscribe(topic)
        # Clean up from DATA topics as necessary
        if(topic in self.puttable_data):
            self.puttable_data.pop(topic)

    def get_subscribable_topics(self):
        return list(self.subscribable_mapping.keys())

    def get_puttable_topics(self):
        return list(self.puttable_mapping.keys())

    def get_gettable_topics(self):
        return list(self.gettable_mapping.keys())

    def get_publishable_topics(self):
        return list(self.publishable_mapping.keys())
