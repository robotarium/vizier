import mqtt_interface.pipeline as pipeline
import mqtt_interface.promise as promise
import mqtt_interface.asyncqueue as asyncqueue
import paho.mqtt.client as mqtt
# import asyncio
import collections
import concurrent.futures
import functools
import queue

# For logging
import logging
import logging.handlers as handlers

#Some named tuples to make things a bit more readable
FutureTask = collections.namedtuple('FutureTask', ['f', 'promise'])

# Filter for logging
class MQTTInterface:
    """
    This is a wrapper around the Paho MQTT interface with enhanced functionality
    """
    def __init__(self, port=1884, keep_alive=60, host="localhost", logging_config = None):
            #Set up MQTT client

            self.host = host
            self.port = port
            self.keep_alive = keep_alive

            #Numer of workers must be equal to the number of "locking" queues + 2...
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
            self.futures = []

            self.client_queue = queue.Queue()
            self.client = mqtt.Client()
            self.client.on_message = self.on_message

            #I shouldn't need a lock for this...
            self.channels = {}
            self.callbacks = {}

            # self.loop = asyncio.get_event_loop()

            # For logging

            if(logging_config):
                logging.configDict(logging_config)
                self.logger = logging.getLogger(__name__)
            else:
                self.logger = logging.getLogger(__name__)
                self.logger.setLevel(logging.DEBUG)

                formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

    def create_on_connect(self, promise):
        """
        Higher order function that creates a callback function to handle MQTT server connection
        """
        def on_connect(client, userdata, flags, rc):
            promise.fulfill(True)
            self.logger.info("Client successfully connected to server.")
            pass

        return on_connect

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        """
        Callback handling messages from the client.  Either puts the message into a callback or a channel
        """
        if(msg.topic in self.callbacks):
            self.callbacks[msg.topic](msg)

        # if(msg.topic in self.channels):
        #     self.channels[msg.topic].put(msg)

    # def _modifyClient(self, f):
    #     '''
    #     Allows the insertion of a function into the client's modification queue.  Returns a future representing
    #     the eventual returned result of the submitted function.
    #     '''
    #
    #     #A promise is really just a queue of size 1
    #     prom = promise.AsyncPromise(self.loop, executor=self.executor)
    #     self.client_queue.put(FutureTask(f, prom))
    #
    #     return prom

    def _modify_client_sync(self, f):
        """
        Modifies the client without asyncio.
        """
        #A promise is really just a queue of size 1
        prom = promise.Promise(executor=self.executor)
        self.client_queue.put(FutureTask(f, prom))

        return prom

    #Create subscribe queue method and subscribe callback method

    def subscribe_with_callback(self, channel, callback):
        """
        Thread safe
        Subscribes to a channel with a callback.  All messages to that channel will be passed into the callback
        """
        #Update should be thread safe...
        self.callbacks.update({channel: callback})

        def clientModification():
            self.client.subscribe(channel)
            return True

        # Try to modify this class
        prom = self._modify_client_sync(clientModification)
        # Wait on the result of the modification
        result = prom.result()

        if not result:
            self.logger.error("Client couldn't successfully subscribe to topic: " + channel)

    # @asyncio.coroutine
    # def subscribe(self, channel):
    #     """
    #     Thread safe
    #     A subscribe routine that yields a queue to which all subsequent messages to the given topic will be passed
    #     """
    #     #Should be thread safe...
    #     self.channels.update({channel: asyncqueue.AsyncQueue(executor=self.executor)})
    #
    #     def clientModification():
    #         self.client.subscribe(channel)
    #         return True
    #
    #     prom = self._modifyClient(clientModification)
    #     result = yield from prom.result()
    #
    #     if not result:
    #         self.logger.error("Client couldn't successfully subscribe to topic: " + channel)

    def subscribe(self, channel):
        """
        Thread safe
        A subscribe routine that yields a queue to which all subsequent messages to the given topic will be passed
        """
        #Should be thread safe...
        new_queue = queue.Queue()
        # new_queue = asyncqueue.AsyncQueue(executor=self.executor)
        def f(msg):
            new_queue.put(msg)

        result = self.subscribe_with_callback(channel, f)
        #self.channels.update({channel: new_queue})

        # def client_modification():
        #     self.client.subscribe(channel)
        #     return True
        #
        # prom = self._modify_client_sync(client_modification)
        # result = prom.result()
        #
        # if not result:
        #     self.logger.error("Client couldn't successfully subscribe to topic: " + channel)

        return (result, new_queue)

    # @asyncio.coroutine
    # def unsubscribe(self, channel):
    #     """
    #     Unsubscribes from a particular channel
    #     """
    #     def clientModification():
    #         self.client.unsubscribe(channel)
    #         return True
    #
    #     prom = self._modifyClient(clientModification)
    #     result = yield from prom.result()
    #
    #     if not result:
    #         self.logger.error("Client couldn't successfully unsubscribe to topic: " + channel)
    #
    #     #Remove duplex channel from list of entities.  Should be thread-safe...
    #     self.channels.pop(channel, None)

    def unsubscribe(self, channel):
        """
        Unsubscribes from a particular channel
        """
        def clientModification():
            self.client.unsubscribe(channel)
            return True

        prom = self._modify_client_sync(clientModification)
        result = prom.result()

        if not result:
            self.logger.error("Client couldn't successfully unsubscribe to topic: " + channel)

        #Remove duplex channel from list of entities.  Should be thread-safe...
        self.channels.pop(channel, None)

        return result

    # @asyncio.coroutine
    # def wait_for_message(self, channel, timeout=60):
    #     """
    #     Asyncio coroutine that waits for a particular message from a channel.
    #     """
    #     message = yield from self.channels[channel].async_get(self.loop, timeout=timeout)
    #     return message
    #
    # @asyncio.coroutine
    # def send_message(self, channel, message):
    #     """
    #     Thread safe
    #     Asyncio-compatible
    #     Sends a message on a particlar channel
    #     """
    #     def clientModification():
    #         self.client.publish(channel, message)
    #         return True
    #
    #     prom = self._modifyClient(clientModification)
    #     result = yield from prom.result()
    #
    #     if not result:
    #         self.logger.error("Client couldn't successfully send message to topic: " + channel)

    def send_message(self, channel, message):

        def client_modification():
            self.client.publish(channel, message)
            return True

        prom = self._modify_client_sync(client_modification)
        result = prom.result()

        if not result:
            self.logger.error("Client couldn't successfully send message to topic: " + channel)

    # def run_pipeline(self, pipeline, exeption_handler=None):
    #     self.loop.set_exception_handler(exeption_handler)
    #     return self.loop.run_until_complete(pipeline)

    def start(self):

        #Wait for client to connect before proceeding
        p = promise.Promise(executor=self.executor)
        self.client.on_connect = self.create_on_connect(p)

        #Attempt to connect the client to the specified broker
        try:
            self.client.connect(self.host, self.port, self.keep_alive)
        except Exception as e:
            self.logger.error("MQTT client couldn't connect to broker at host: " + repr(self.host) + " port: " + repr(self.port))
            raise e

        # Starts MQTT client in background thread
        self.client.loop_start()

        #Block on connection resolving
        p.result()

        #Loop to handle modifications to client
        def client_thread():
            while True:
                result = self.client_queue.get()
                if result is None:
                    break

                #Else if not poison-pilled
                try:
                    result.promise.fulfill(result.f())
                except Exception as e:
                    print("Encountered exception " + repr(e) + " in clientQ")
                    result.promise.fulfill(e)

            return True

        self.futures.append(self.executor.submit(client_thread))


    def stop(self):
        #Stops MQTT client
        self.client.loop_stop()
        self.client_queue.put(None)

        results = [f.result(timeout=5) for f in self.futures]

        if not any(results):
            #If any of the results are NOT true
            print("Encountered error handling a future.  You should inspect the futures array to ensure everything is functioning")

        self.executor.shutdown()
