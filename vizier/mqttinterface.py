import paho.mqtt.client as mqtt
import queue
import threading

# For logging
import logging


# CountDownLatch for some MQTT client checking
class _CountDownLatch():
    """This class handles some synchronization behind starting the paho MQTT client

    Attributes:
        _cv (threading.Condition): Condition variable for waiting on the countdown
        _count (int): Current count of the latch.  Calls to wait return when count reaches 0
    """

    def __init__(self, count=1):
        self._cv = threading.Condition()
        self._count = count

    def _counted_down(self):
        """Helper function to determine if the countdown has occured"""

        return self._count <= 0

    def count_down(self):
        """Thread safe.  When the count reaches 0, all waits return"""

        with self._cv:
            self._count -= 1
            self._count = max(self._count, 0)
            if(self._count <= 0):
                self._cv.notify_all()

    def wait(self, timeout=None):
        """Thread safe.  Waits for the count to reach 0

        Args:
            timeout (double): timeout to wait on latch
        """

        with self._cv:
            self._cv.wait_for(self._counted_down, timeout=timeout)


class MQTTInterface:
    """This is a wrapper around the Paho MQTT interface with enhanced functionality

    Attributes:
        host (str): The MQTT broker's host to which this client connects.
        port (int): The MQTT broker's port to which this client connects.
    """

    def __init__(self, port=1884, keep_alive=60, host="localhost", logging_config=None):
            # Set up MQTT client
            self.host = host
            self.port = port
            self.keep_alive = keep_alive

            # Re-entrant lock for the various methods
            self.lock = threading.Lock()

            # self.client_queue = queue.Queue()
            self.client = mqtt.Client()
            self.client.on_message = self.on_message

            # I shouldn't need a lock for this...
            self.channels = {}
            self.callbacks = {}

            # For logging
            if(logging_config):
                logging.configDict(logging_config)
                self.logger = logging.getLogger(__name__)
            else:
                self.logger = logging.getLogger(__name__)
                self.logger.setLevel(logging.DEBUG)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        """Thread safe. Callback handling messages from the client.  Either puts the message into a callback or a channel

        Args:
            client: Client from which message was recieved
            userdata: Data about the client
            msg: MQTT payload
        """

        # Unsubscribe could happen between these statements, so we need the lock
        with self.lock:
            if(msg.topic in self.callbacks):
                # Just pass the byte payload directly to the callback, since the other info is more or less useless
                self.callbacks[msg.topic](msg.payload)

    def subscribe_with_callback(self, channel, callback):
        """Thread safe.  Subscribes to a channel with a callback using the underlying MQTT client.

        All messages to that channel will be passed into the callback

        Args:
            channel (str): Channel to which the node subscribes
            callback (function): Callback function for the topic
        """

        with self.lock:
            self.callbacks.update({channel: callback})
            self.client.subscribe(channel)

    def subscribe(self, channel):
        """Thread safe. A subscribe routine that yields a queue to which all subsequent messages to the given topic will be passed

        Args:
            channel (str): Channel to which the client will subscribe

        Returns:
            A queue containing all future messages from the supplied channel
        """

        # Should be thread safe, since locking is handled in subscribe_with_callback
        q = queue.Queue()

        def f(msg):
            nonlocal q
            q.put(msg)

        self.subscribe_with_callback(channel, f)

        return q

    def unsubscribe(self, channel):
        """Thread safe. Unsubscribes from a particular channel

        Args:
            channel (str): Channel from which the client unsubscribes
        """

        with self.lock:
            self.client.unsubscribe(channel)
            self.channels.pop(channel, None)

    def send_message(self, channel, message):
        """Thread safe.  Sends a message on the MQTT client.

        Args:
            channel (str): string (channel on which to send message)
            message (bytes): Message to be sent.  Should be in an encoded bytes format (like UTF-8)
        """

        # TODO: Ensure that this function is actually thread-safe
        self.client.publish(channel, message)

    def start(self, timeout=None):
        """Handles starting the underlying MQTT client"""

        cdl = _CountDownLatch(1)

        # Local function to handle connection to the MQTT server
        def on_connect(client, userdata, flags, rc):
            nonlocal cdl
            self.logger.info('MQTT client successfully connected to broker on host: {0}, port: {1}'.format(self.host, self.port))
            cdl.count_down()

        self.client.on_connect = on_connect

        # Attempt to connect the client to the specified broker
        try:
            self.client.connect(self.host, self.port, self.keep_alive)
        except Exception as e:
            self.logger.error('MQTT client could not connect to broker at host: {0}, port: {1}'.format(self.host, self.port))
            raise e

        # Starts MQTT client in background thread.  This has to be done before the client will process any messages
        self.client.loop_start()

        # Have to start client before we wait on CDL.  Client won't process any messages until we start it
        cdl.wait(timeout=timeout)

    def stop(self):
        """Handles stopping the MQTT client"""

        # Stops MQTT client
        self.client.loop_stop()
