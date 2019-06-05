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
    """Creates a node on a vizier network.

    Attributes:

        _host (str): Host of the MQTT broker.
        _port (int): Port of the MQTT broker.
        _mqtt_client: Underlying Paho MQTT client.
        _node_descriptor (dict): JSON-formmated dict containing information about the node.  For example,

            .. code-block:: python

                {
                    'end_point': 'node'
                    'links':
                    {
                        '/link': {'type': 'STREAM'}
                    }
                    'requests':
                    [
                        {
                            'link': 'node_b/link',
                            'type': 'DATA',
                            'required': false
                        }
                    ]
                }

        _end_point (str): Endpoint of the node.
        _expanded_links (dict): JSON-formatted dict containing the recursively expanded links.  For example, the above node descriptor would expand to

            .. code-block:: python

                {
                    'node/link': {'type': 'STREAM'}
                }

        _requested_links (dict): JSON-formatted dict containing the requests for the node.  For example, the request from the above node descriptor would
        be

            .. code-block:: python

                {
                    'node_b/link': {'link': 'node_b/link', 'type': 'DATA', 'required': false}
                }

        _request_channel (str): Channel on which requests are made.  Always <end_point>/'requests'.
        _logger (logging.Logger): Logger for the node.
        puttable_links (list): List of links to which data may be put.  These links are the node's links that are of type DATA.
        publishable_links (list):  List of links to which data may be published.  These links are the node's links that are of type STREAM.
        gettable_links (list): List of links from which data may be retrieved.  These links are the node's requested links that are of type DATA.
        subscribable_links (list):  List of links to which the node can subscribe.  These links are the node's requested links that are of type STEAM.

    """

    def __init__(self, host, port, node_descriptor, max_workers=20):

        # Executor for parallelizing requests
        self._executor = futures.ThreadPoolExecutor(max_workers=max_workers)

        # Setting up MQTT client as well as the dicts to hold DATA information
        self._host = host
        self._port = port
        self._mqtt_client = mqtt.MQTTInterface(port=self._port, host=self._host)
        self._node_descriptor = node_descriptor

        # Store the end point of the node for convenience
        self._end_point = node_descriptor['end_point']

        # Recursively expand the links from the provided descriptor file
        # All data regarding the link, including the body, is stored in this dictionary.  Various requests will
        # usually access it to retrieve this data
        self._expanded_links, self._requested_links = utils.generate_links_from_descriptor(self._node_descriptor)

        # By convention, the node descriptor is always on this link
        self._expanded_links[self._end_point + '/node_descriptor'] = {'type': 'DATA', 'body': json.dumps(self._node_descriptor)}

        # Channel on which requests are received
        self._request_channel = utils.create_request_link(self._end_point)

        # Logging
        self._logger = log.get_logger()

        # Figure out which topics are providing etc...
        # Make sure to remove <node>/node_descriptor as a puttable topic, as this is dedicated
        self.puttable_links = {x for x, y in self._expanded_links.items() if y['type'] == 'DATA'} - {self._end_point + '/node_descriptor'}
        self.publishable_links = {x for x, y in self._expanded_links.items() if y['type'] == 'STREAM'}

        # Parse out data/stream topics
        self.gettable_links = {x for x, y in self._requested_links.items() if y['type'] == 'DATA'}
        self.subscribable_links = {x for x, y in self._requested_links.items() if y['type'] == 'STREAM'}

    def _make_request(self, method, link, body, request_id=None, attempts=15, timeout=0.25):
        """Makes a request for data on a particular topic.  The exact action depends on the specified method.

        Args:
            method (str): Method for request (e.g., 'GET').
            link (str): Link on which to make request.
            body (dict): JSON-formatted dict representing the body of the message.
            request_id (str, optional): Unique request ID for this message.
            attempts (int, optional): Number of times to retry the request, if it times out.
            timeout (double, optional): Timeout to wait for return message on each request.

        Returns:
            A JSON-formatted dict representing the contents of the message.

        """

        # If we didn't get a request_id, create one
        if not request_id:
            request_id = utils.create_message_id(self._end_point)

        # Set up request/response link for this request
        to_node = link.split('/')[0]
        request_link = utils.create_request_link(to_node)
        response_link = utils.create_response_link(to_node, request_id)

        q = self._mqtt_client.subscribe(response_link)
        decoded_message = None
        encoded_request = json.dumps(utils.create_request(request_id, method, link, body)).encode(encoding='UTF-8')

        # Repeat request a number of times based on the specified number of attempts
        for _ in range(attempts):
            self._mqtt_client.send_message(request_link, encoded_request)

            # We expect this to potentially fail with a timeout
            try:
                mqtt_message = q.get(timeout=timeout)
            except Exception:
                # Don't try to decode message if we didn't get anything
                self._logger.info('Retrying (%s) request for node (%s) for link (%s)' % (method, to_node, link))
                continue

            # Try to decode packet.  Could potentially fail
            try:
                decoded_message = json.loads(mqtt_message.decode(encoding='UTF-8'))
                break
            except Exception:
                # Just pass here because we expect to fail.  In the future,
                # split the exceptions up into reasonable cases
                self._logger.error('Could not decode network message')

        if(decoded_message is None):
            self._logger.error('Get request on topic ({}) failed'.format(link))

        # Make sure that we unsubscribe from the response channel, and, finally, return the decoded message
        self._mqtt_client.unsubscribe(response_link)

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
            self._logger.error('Received undecodable network message in request handler')
            self._logger.error(repr(e))
            encountered_error = True

        # Check to make sure that it's a valid request
        # TODO: Handle error in a more specific way
        if('id' not in decoded_message):
            # This is an error.  Return from the callback
            self._logger.error('Request received without valid id')
            encountered_error = True
        else:
            message_id = decoded_message['id']

        if('method' not in decoded_message):
            # This is an error.  Return from the callback
            self._logger.error('Request received without valid method')
            encountered_error = True
        else:
            method = decoded_message['method']

        if('link' not in decoded_message):
            # This is an error.  Return from the callback
            self._logger.error('Request received without valid link')
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
            self._logger.info('Received GET request for topic %s' % requested_link)
            self._logger.info(repr(decoded_message))
            # Handle the get request by looking for information under the specified URI
            if(requested_link in self._expanded_links):
                # If we have any record of this URI, create a response message
                response = utils.create_response(_http_codes['success'],
                                                 self._expanded_links[requested_link]['body'], self._expanded_links[requested_link]['type'])
                response_channel = utils.create_response_link(self._end_point, message_id)
                self._mqtt_client.send_message(response_channel, json.dumps(response).encode(encoding='UTF-8'))

        # TODO: Handle other methods

    def put(self, link, info):
        """Puts data on a particular link.

        This data is retrievable from other nodes via a GET request.

        Args:
            link (str): Link on which to place data.
            info (dict): JSON-formatted dict representing gettable data.

        Raises:
            ValueError: If the provided link is not classified as DATA.

        """

        # Ensure type of info
        if(type(info) is not str):
            error_msg = 'Type of info must be str was ({})'.format(type(info))
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # Ensure that the link has been specified by the node descriptor

        dedicated = self._end_point + '/node_descriptor'
        if(link is dedicated):
            error_msg = 'Cannot PUT to dedicated link ({})'.format(dedicated)
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        if(link in self.puttable_links):
            # This operation should be threadsafe (for future reference)
            # Ensure that the link is of type DATA
            self._expanded_links[link]['body'] = info
        else:
            error_msg = 'Link ({0}) not in puttable links ({1})'.format(link, self.puttable_links)
            self._logger.error(error_msg)
            raise ValueError(error_msg)

    def publish(self, link, data):
        """Publishes data on a particular link.  Link should have been classified as STREAM in node descriptor.

        Args:
            link (str): Link on which data is published.
            data (bytes): Bytes to be published over MQTT.

        Raises:
            ValueError: If the provided link is not classified as STREAM.

        """

        if(link in self.publishable_links):
            self._mqtt_client.send_message(link, data)
        else:
            error_msg = 'Link ({0}) not contained in publishable links ({1})'.format(link, self.publishable_links)
            self._logger.error(error_msg)
            raise ValueError(error_msg)

    def get(self, link, timeout=0.20, attempts=5):
        """Make a get request on a particular link, provided that the link is in the gettable links for the node.

        Args:
            link (str): Link on which GET request is made.
            timeout (double): Timeout for GET request.
            attempts (int): Number of times to attempt each GET request.

        Returns:
            Data that was retrieved from the link as a JSON-formatted dict.

        Raises:
            ValueErorr: If link is not classified as gettable (remote DATA).

        """

        if(link in self.gettable_links):
            response = self._make_request('GET', link, {}, timeout=timeout, attempts=attempts)
            if response is None:
                return
            else:
                return response['body']
        else:
            error_msg = 'Link ({0}) not contained in gettable links ({1})'.format(link, self.gettable_links)
            self._logger.error(error_msg)
            raise ValueError(error_msg)

    def subscribe(self, link):
        """Subscribes to the provided link with the underlying MQTT client, provided that the link is in the subscribable links for the node.

        Args:
            link (str): Link to which the node should subscribe.

        Raises:
            ValueError: If link is not classified as subscrbable (remote STREAM).

        """

        if(link in self.subscribable_links):
            q = self._mqtt_client.subscribe(link)
            return q
        else:
            raise ValueError('Link ({0}) not contained in subscribable_links ({1})'.format(link, self.subscribable_links))

    def subscribe_with_callback(self, link, callback):
        """Subscribes to link with the callback using the underlying MQTT client.

        Args:
            link (str): Link to which the client subscribes.
            callback (function): All messages recieved on link are passed through this function.

        Raises:
            ValueError: If the provided link is not subscribable (remote STREAM).

        """

        if(link in self.subscribable_links):
            self._mqtt_client.subscribe_with_callback(link, callback)
        else:
            raise ValueError('Link ({0}) not contained in subscribable links ({1})'.format(link, self.subscribable_links))

    def unsubscribe(self, link):
        """If the specified link is in the subscribable links, the node unsubscribes from the link.

        Args:
            link (str): Link from which the node unsubscribes.

        Raises:
            ValueError: If the provided link is not subscribable (remote STREAM).

        """

        if(link in self.subscribable_links):
            self._mqtt_client.unsubscribe(link)
        else:
            raise ValueError('Link ({0}) not contained in subscribable links ({1})'.format(link, self.subscribable_links))

    def verify_dependencies(self, attempts=10, timeout=0.25):
        """Verifies the node's dependencies.  In particular, it attempts to make a GET request for each required request in the node descriptor.

        Args:
            attempts (int): number of times to attempt the GET requests.
            timeout (float): timeout for the GET requests in seconds.

        Raises:
            ValueError: If the requests were not present on the network.

        """

        # Executor for handling multiple GET requests
        # Get required requests.  Key 'required' will be present due to prior parsing
        required_links = [x for x, y in self._requested_links.items() if y['required']]
        receive_results = dict(zip(required_links, self._executor.map(lambda x: self._make_request('GET', x, {}, timeout=timeout, attempts=attempts),
                                                                      required_links)))

        # Ensure that all required links were obtained
        deps_satisfied = True
        failed_to_get = []
        for x, y in receive_results.items():
            if y is None:
                # At this point, 'required' key is definitely included from the parsing, so we can just check for it
                if(self._requested_links[x]['required']):
                    deps_satisfied = False
                    failed_to_get.append(x)
                else:
                    self._logger.error('Could not get required link ({})'.format(x))

        # If one of the required links couldn't be obtained, throw an error
        if (not deps_satisfied):
            raise ValueError('Could not get required links ({})'.format(failed_to_get))

        self._logger.info('Succesfully connected to vizier network')

    def start(self, attempts=10, timeout=0.25):
        """Start the MQTT client and connect to the vizier network

        Args:
            attempts (int):  Number of times to attempt each GET request.
            timeout (double): Timeout for each GET Request.

        Raises:
            ValueError: If all required links were not available on the network.

        """

        # Start the MQTT client to ensure we can attach this callback
        self._mqtt_client.start()

        # Subscribe to requests channel with request handler
        self._mqtt_client.subscribe_with_callback(self._request_channel, self._handle_request)

        self.verify_dependencies(attempts=attempts, timeout=timeout)

    def stop(self):
        """Stop the MQTT client"""

        self._mqtt_client.stop()
