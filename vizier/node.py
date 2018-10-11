import vizier.mqttinterface as mqtt
import concurrent.futures as futures
import json
import vizier.utils as utils
import vizier.log as log

# HTTP codes for convenience later
_http_codes = {'success': 200, 'not_found': 404}


# TODO: Data should be in byte format
# TODO: Let remote nodes do a put on links?

class Node:
    """ Represents a node on the vizier network...

    More stuff
    Attributes:
        mqtt_client: Underlying Paho MQTT client
        node_descriptor (dict): JSON-formmated dict used containing information about the node
        ...

    """

    def __init__(self, broker_host, broker_port, node_descriptor, logger=None, max_workers=20):

        # Executor for parallelizing requests
        self.executor = futures.ThreadPoolExecutor(max_workers=max_workers)

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
        self.expanded_links[self.end_point + '/node_descriptor'] = {'type': 'DATA', 'body': node_descriptor}

        self.host = broker_host
        self.port = broker_port

        # Figure out which topics are providing etc...
        # Make sure to remove <node>/node_descriptor as a puttable topic, as this is dedicated
        self.puttable_links = {x for x, y in self.expanded_links.items() if y['type'] == 'DATA'} - {self.end_point+'/node_descriptor'}
        self.publishable_links = {x for x, y in self.expanded_links.items() if y['type'] == 'STREAM'}

        # Parse out data/stream topics
        self.gettable_links = {x for x, y in self.requested_links.items() if y['type'] == 'DATA'}
        self.subscribable_links = {x for x, y in self.requested_links.items() if y['type'] == 'STREAM'}

        # Channel on which requests are received
        self.request_channel = utils.create_request_link(self.end_point)

        self.logger = log.get_logger()

    def _make_request(self, method, link, body, request_id=None, attempts=15, timeout=0.25):
        """Makes a request for data on a particular topic.  The exact action depends on the specified method

        Args:
            method (str): Method for request (e.g., 'GET')
            link (str): Link on which to make request
            body (dict): JSON-formatted dict representing the body of the message
            request_id (str): Unique request ID for this message
            attempts (int): Number of times to retry the request, if it times out
            timeout (double): Timeout to wait for return message on each request

        Returns:
            A JSON-formatted dict representing the contents of the message.

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

        # Repeat request a number of times based on the specified number of attempts
        for _ in range(attempts):
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
                decoded_message = json.loads(mqtt_message.decode(encoding='UTF-8'))
                break
            except Exception:
                # Just pass here because we expect to fail.  In the future,
                # split the exceptions up into reasonable cases
                self.logger.error('Could not decode network message')

        if(decoded_message is None):
            self.logger.error('Get request on topic ({}) failed'.format(link))

        # Make sure that we unsubscribe from the response channel, and, finally, return the decoded message
        self.mqtt_client.unsubscribe(response_link)

        return decoded_message

    def _handle_request(self, network_message):
        """Private function for handling incoming network requests.  All requests on the channel <node_name>/requests
        are passed to this function.  Then, responses are returned on the channel <node_name>/responses/<message_id>.

        Args:
            network_message (bytes): A UTF-8-encoded string representing a JSON-formatted request message.

        """

        # Make request handler for vizier network.  Later this function is attached as a callback to
        # <node_name>/requests to handle incoming requests.  All requests are passed through this function
        encountered_error = False
        try:
            decoded_message = json.loads(network_message.decode(encoding='UTF-8'))
        except Exception as e:
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
            # response = utils.create_response(_http_codes['not_found'], {"error": "Encountered error in request"}, "ERROR")
            # response_channel = utils.create_response_link(self.end_point, message_id)
            return

        # We have a valid request at this point
        if(method == 'GET'):
            self.logger.info('Received GET request for topic %s' % requested_link)
            self.logger.info(repr(decoded_message))
            # Handle the get request by looking for information under the specified URI
            if(requested_link in self.expanded_links):
                # If we have any record of this URI, create a response message
                response = utils.create_response(_http_codes['success'],
                                                 self.expanded_links[requested_link]['body'], self.expanded_links[requested_link]['type'])
                response_channel = utils.create_response_link(self.end_point, message_id)
                self.mqtt_client.send_message(response_channel, json.dumps(response).encode(encoding='UTF-8'))

            # TODO: Handle other methods?

    def put(self, link, info):
        """Puts data on a particular link

        This data is retrievable from other nodes via a GET request

        Args:
            link (str): Link on which to place data
            info (dict): JSON-formatted dict representing gettable data

        Raises:
            ValueError: If the provided link is not classified as DATA

        """

        # Ensure that the link has been specified by the node descriptor
        if(link in self.puttable_links):
            # This operation should be threadsafe (for future reference)
            # Ensure that the link is of type DATA
            self.expanded_links[link]['body'] = info
            # self.logger.info('Put something on link %s' % link)
        else:
            # TODO: Handle error
            raise ValueError

    def publish(self, link, data):
        """Publishes data on a particular link.  Link should have been classified as STREAM in node descriptor.

        Args:
            link (str): Link on which data is published
            data (bytes): Bytes to be published over MQTT

        Raises:
            ValueError: If the provided link is not classified as STREAM

        """

        if(link in self.publishable_links):
            self.mqtt_client.send_message(link, data)
        else:
            self.logger.error('Requested link (%s) not classified as STREAM' % link)
            raise ValueError

    def get(self, link, timeout=0.20, attempts=5):
        """Make a get request on a particular link, provided that the link is in the gettable links for the node.

        Args:
            link (str): Link on which GET request is made
            timeout (double): Timeout for GET request
            attempts (int): Number of times to attempt each GET request

        Returns:
            Data that was retrieved from the link as a JSON-formatted object

        Raises:
            ValueErorr: If link is not classified as gettable (remote DATA)

        """

        if(link in self.gettable_links):
            response = self._make_request('GET', link, {}, timeout=timeout, attempts=attempts)
            if response is None:
                return
            else:
                return response['body']
        else:
            raise ValueError('Link ({0}) not contained in gettable links ({1})'.format(link, self.gettable_links))

    def subscribe(self, link):
        """Subscribes to the provided link with the underlying MQTT client, provided that the link is in the subscribable links for the node.

        Args:
            link: string (link to which the node should subscribe)

        Raises:
            ValueError: If link is not classified as subscrbable (remote STREAM)

        """

        if(link in self.subscribable_links):
            q = self.mqtt_client.subscribe(link)
            return q
        else:
            raise ValueError('Link ({0}) not contained in subscribable_links ({1})'.format(link, self.subscribable_links))

    def subscribe_with_callback(self, link, callback):
        """Subscribes to link with the callback using the underlying MQTT client.

        Args:
            link (str): Link to which the client subscribes
            callback (function): All messages recieved on link are passed through this function

        Raises:
            ValueError: If the provided link is not subscribable (remote STREAM)

        """
        if(link in self.subscribable_links):
            self.mqtt_client.subscribe_with_callback(link, callback)
        else:
            raise ValueError('Link ({0}) not contained in subscribable_links ({1})'.format(link, self.subscribable_links))

    def unsubscribe(self, link):
        """If the specified link is in the subscribable links, the node unsubscribes from the link.

        Args:
            link (str): Link from which the node unsubscribes

        Raises:
            ValueError: If the provided link is not subscribable (remote STREAM)

        """

        if(link in self.subscribable_links):
            self.mqtt_client.unsubscribe(link)
        else:
            raise ValueError('Link ({0}) not contained in subscribable_links ({1})'.format(link, self.subscribable_links))

    def verify_dependencies(self, attempts=10, timeout=0.25):
        """Verifies the node's dependencies.  In particular, it attempts to make a GET request for each required request in the node descriptor

        Args:
            attempts (int): number of times to attempt the GET requests
            timeout (float): timeout for the GET requests in seconds

        Returns:
            True if the required dependencies were present on the network

        Raises:
            ValueError: If the requests were not present on the network

        """

        # Executor for handling multiple GET requests
        # Get required requests.  Key 'required' will be present due to prior parsing
        required_links = [x for x, y in self.requested_links.items() if y['required']]
        receive_results = dict(zip(required_links, self.executor.map(lambda x: self._make_request('GET', x, {}, timeout=timeout, attempts=attempts),
                                                                     required_links)))

        # Ensure that all required links were obtained
        deps_satisfied = True
        failed_to_get = []
        for x, y in receive_results.items():
            if y is None:
                # At this point, 'required' key is definitely included from the parsing, so we can just check for it
                if(self.requested_links[x]['required']):
                    deps_satisfied = False
                    failed_to_get.append(x)
                else:
                    self.logger.error('Could not get required request(s) {}'.format(x))

        # If one of the required links couldn't be obtained, throw an error
        if (not deps_satisfied):
            raise ValueError('Could not get required links {}'.format(failed_to_get))

        self.logger.info('Succesfully connected to vizier network')

        return deps_satisfied

    def start(self, attempts=10, timeout=0.25):
        """Start the MQTT client and connect to the vizier network

        Args:
            attempts (int):  Number of times to attempt each GET request
            timeout (double): Timeout for each GET Request

        Raises:
            ValueError: If all requests were not available on the network

        """

        # Start the MQTT client to ensure we can attach this callback
        self.mqtt_client.start()

        # Subscribe to requests channel with request handler
        self.mqtt_client.subscribe_with_callback(self.request_channel, self._handle_request)

        return self.verify_dependencies(attempts=attempts, timeout=timeout)

    def stop(self):
        """Stop the MQTT client"""

        self.mqtt_client.stop()
