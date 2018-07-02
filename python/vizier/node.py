import vizier.mqttinterface as mqtt
import concurrent.futures as futures
import json
from vizier import utils

# For logging
import logging

#TODO: set logging in a better way
logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

# HTTP codes for convenience later
_http_codes = {'success': 200, 'not_found': 404}


class Node:

    def __init__(self, broker_host, broker_port, node_descriptor, logging_config=None):

        # Setting up MQTT client as well as the dicts to hold DATA information
        self.mqtt_client = mqtt.MQTTInterface(port=broker_port, host=broker_host)
        self.node_descriptor = node_descriptor
        # Store the end point of the node for convenience
        self.end_point = node_descriptor['end_point']

        # Recursively expand the links from the provided descriptor file
        # All data regarding the link, including the body, is stored in this dictionary.  Various requests will
        # usually access it to retrieve this data
        self.expanded_links, self.requested_links = utils.generate_links_from_descriptor(node_descriptor)
        # By convention, the node descriptor is always on this link
        self.expanded_links[self.end_point+'/node_descriptor'] = {'type': 'DATA', 'body': node_descriptor}
        self.host = broker_host
        self.port = broker_port

        # Figure out which topics are providing etc...
        # Make sure to remove <node>/node_descriptor as a puttable topic, as this is dedicated
        self.puttable_links = set(filter(lambda x: self.expanded_links[x]['type'] == 'DATA', self.expanded_links)) - {self.end_point+'/node_descriptor'}
        self.publishable_links = set(filter(lambda x: self.expanded_links[x]['type'] == 'STREAM', self.expanded_links))
        # The next two values are set later in the 'connect' method
        self.gettable_links = set()
        self.subscribable_links = set()

        # Channel on which requests are received
        self.request_channel = utils.create_request_link(self.end_point)

        # Define an executor for making requests
        self.executor = None

        # Some logging definitions.
        # TODO: This probably needs to change at some point.
        if(logging_config):
            logging.configDict(logging_config)
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.DEBUG)

    def _make_request(self, method, link, body, request_id=None, retries=30, timeout=1):
        """Makes a get request for data on a particular topic.
        Will try 'retries' amount for 'timeout' seconds.
        method: string (method for request)
        link: string (link on which to make request)
        body: JSON-formatted dict (dict containing optional body information for the request)
        request_id: string (uniquely generated key for this message)
        retries: int (optional number of retries for the request)
        timeout: double (optimal timeout to wait for return message)

        -> MQTT message (containing the response to the request)
        """

        # If we didn't get a request_id, create one
        if not request_id:
            request_id = utils.create_message_id()

        # Set up request/response link for this request
        tokens = link.split('/')
        to_node = tokens[0]
        request_link = utils.create_request_link(to_node)
        response_link = utils.create_response_link(to_node, request_id)

        q = self.mqtt_client.subscribe(response_link)
        decoded_message = None
        encoded_request = json.dumps(utils.create_request(request_id, method, link, body)).encode(encoding='UTF-8')

        # Repeat request a number of times based on the specified number of retries
        for _ in range(retries):
            self.mqtt_client.send_message(request_link, encoded_request)
            # We expect this to potentially fail with a timeout
            try:
                mqtt_message = q.get(timeout=timeout)
            except Exception:
                # Don't try to decode message if we didn't get anything
                self.logger.info('Retrying (%s) request for node (%s) for link (%s)' % (method, to_node, link))
                continue

            # Try to decode packet.  Could potentially fail
            try:
                decoded_message = json.loads(mqtt_message.payload.decode(encoding='UTF-8'))
                break
            except Exception:
                # Just pass here because we expect to fail.  In the future,
                # split the exceptions up into reasonable cases
                self.logger.error('Couldn\'t decode network message')
                pass

        if(decoded_message is None):
            self.logger.error('Get request on topic ({}) failed'.format(link))

        # Make sure that we unsubscribe from the response channel, and, finally, return the decoded message
        self.mqtt_client.unsubscribe(response_link)
        return decoded_message

    def put(self, link, info):
        """Offers data on a particular link
        link: string (link on which to place data)
        info: dict (data to place on the link)

        -> None"""

        # Ensure that the link has been specified by the node descriptor
        if(link in self.puttable_links):
            # This operation should be threadsafe (for future reference)
            # Ensure that the link is of type DATA
            self.expanded_links[link]['body'] = info
            self.logger.info('Put something on link %s' % link)
        else:
            # TODO: Handle error
            raise ValueError

    def publish(self, link, data):
        """Publishes data on a particular link.  Link should have been classified as STREAM in node descriptor.
        link: string (link on which data is published)
        data: bytes (encoded bytes to be published)

        -> None"""

        if(link in self.publishable_links):
            self.mqtt_client.send_message(link, data)
        else:
            self.logger.error('Requested link (%s) not classified as STREAM' % link)
            raise ValueError

    def get(self, link, timeout=1, retries=5):
        """Make a get request on a particular link
        
        """
        response = self._make_request('GET', link, {}, timeout=timeout, retries=retries)
        return response['body']

    def subscribe(self, link):
        """

        """
        if(link in self.subscribable_links):
            q = self.mqtt_client.subscribe(link)
            return q
        else: 
            raise ValueError('Link ({0}) not contained in subscribable_links ({1})'.format(link, self.subscribable_links))

    def subscribe_with_callback(self, link, callback):
        if(link in self.subscribable_links):
            self.mqtt_client.subscribe_with_callback(link, callback)
        else:
            raise ValueError('Link ({0}) not contained in subscribable_links ({1})'.format(link, self.subscribable_links))

    def unsubscribe(self, link):
        """
        
        """
        if(link in self.subscribable_links):
            self.mqtt_client.unsubscribe(link)
        else:
            raise ValueError('Link ({0}) not contained in subscribable_links ({1})'.format(link, self.subscribable_links))

    def start(self, retries=5, timeout=1):
        """Start the MQTT client and connect to the vizier network
        retries: int (number of retries for each GET request)
        timeout: double (timeout for each GET request)

        -> None"""

        # Start the MQTT client to ensure we can attach this callback
        self.mqtt_client.start()

        # Make request handler for vizier network.  Later this function is attached as a callback to 
        # <node_name>/requests to handle incoming requests.  All requests are passed through this function
        def request_handler(network_message):
            encountered_error = False
            try:
                decoded_message = json.loads(network_message.payload.decode(encoding='UTF-8'))
            except Exception as e:
                # TODO: Put actual logging here
                self.logger.error('Received undecodable network message in request handler')
                self.logger.error(repr(e))
                encountered_error = True

            # Check to make sure that it's a valid request
            # TODO: Handle error in a more specific way
            if('id' not in decoded_message):
                # This is an error.  Return from the callback
                self.logger.error("Request received without valid id")
                encountered_error = True
            else:
                message_id = decoded_message['id']

            if('method' not in decoded_message):
                # This is an error.  Return from the callback
                self.logger.error('Request received without valid method')
                encountered_error = True
            else:
                method = decoded_message['method']

            if('link' not in decoded_message):
                # This is an error.  Return from the callback
                self.logger.error('Request received without valid link')
                encountered_error = True
            else:
                requested_link = decoded_message['link']

            if(encountered_error):
                # TODO: Create and send error message
                #response = utils.create_response(_http_codes['not_found'], {"error": "Encountered error in request"}, "ERROR")
                #response_channel = utils.create_response_link(self.end_point, message_id)
                return

            # We have a valid request at this point
            if(method == 'GET'):
                self.logger.info('Received GET request for topic %s' % requested_link)
                self.logger.info(repr(decoded_message))
                # Handle the get request by looking for information under the specified URI
                if(requested_link in self.expanded_links):
                    # If we have any record of this URI, create a response message
                    response = utils.create_response(_http_codes['success'], self.expanded_links[requested_link]['body'], self.expanded_links[requested_link]['type'])
                    response_channel = utils.create_response_link(self.end_point, message_id)
                    self.mqtt_client.send_message(response_channel, json.dumps(response).encode(encoding='UTF-8'))

                # TODO: Fill in other methods

        # Subscribe to requests channel with created request handler
        self.mqtt_client.subscribe_with_callback(self.request_channel, request_handler)

        # Executor for handling multiple GET requests
        # This may (or may not) be necessary
        self.executor = futures.ThreadPoolExecutor(max_workers=100)
        receive_results = dict(zip(self.requested_links, self.executor.map(lambda x: self._make_request('GET', x, {}, timeout=timeout, retries=retries), self.requested_links)))

        # Ensure that we got all the results we expected
        if(None in receive_results.items()):
            raise ValueError('Could not get all receive requests')

        #self.logger.info(repr(receive_results))
        self.logger.info('Succesfully connected to vizier network')

        # Parse out data/stream topics
        self.gettable_links = {x for x, y in receive_results.items() if y['type'] == 'DATA'}
        self.subscribable_links = {x for x, y in receive_results.items() if y['type'] == 'STREAM'}

    def stop(self):
        """Stop the MQTT client"""

        self.mqtt_client.stop()
