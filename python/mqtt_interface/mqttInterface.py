import mqtt_interface.pipeline as pipeline
import mqtt_interface.promise as promise
import mqtt_interface.asyncqueue as asyncqueue
import paho.mqtt.client as mqtt
import asyncio
import collections
import concurrent.futures
import functools
import queue

#Some named tuples to make things a bit more readable
FutureTask = collections.namedtuple('FutureTask', ['f', 'promise'])

class MQTTInterface:

    def __init__(self, port=1884, keepAlive=60, host="localhost"):
            #Set up MQTT client

            #Numer of workers must be equal to the number of "locking" queues + 2...
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
            self.futures = []

            self.clientQueue = queue.Queue()
            self.client = mqtt.Client()
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.connect(host, port, keepAlive)

            #I shouldn't need a lock for this...
            self.channels = {}
            self.callbacks = {}

            self.loop = asyncio.get_event_loop()

    def on_connect(self, client, userdata, flags, rc):
        print("Successfully connected to MQTT broker with result code "+str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        #client.subscribe("$SYS/#")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):

        if(msg.topic in self.callbacks):
            self.callbacks[msg.topic](msg)

        if(msg.topic in self.channels):
            self.channels[msg.topic].put(msg)

    def _modifyClient(self, f):
        '''
        Allows the insertion of a function into the client's modification queue.  Returns a future representing
        the eventual returned result of the submitted function.
        '''

        #A promise is really just a queue of size 1
        prom = promise.AsyncPromise(self.loop, executor=self.executor)
        self.clientQueue.put(FutureTask(f, prom))

        return prom

    def _modify_client_sync(self, f):
        #A promise is really just a queue of size 1
        prom = promise.Promise(executor=self.executor)
        self.clientQueue.put(FutureTask(f, prom))

        return prom

    #Create subscribe queue method and subscribe callback method

    def subscribe_with_callback(self, channel, callback):
        #Update should be thread safe...
        self.callbacks.update({channel: callback})

        def clientModification():
            self.client.subscribe(channel)
            return True

        prom = self._modify_client_sync(clientModification)
        result = prom.result()

        if not result:
            print("Client didn't subscribe successfully...")

    @asyncio.coroutine
    def subscribe(self, channel):
        #Should be thread safe...
        self.channels.update({channel: asyncqueue.AsyncQueue(executor=self.executor)})

        def clientModification():
            self.client.subscribe(channel)
            return True

        prom = self._modifyClient(clientModification)
        result = yield from prom.result()

        if not result:
            print("Client didn't subscribe successfully...")

    @asyncio.coroutine
    def unsubscribe(self, channel):
        def clientModification():
            self.client.unsubscribe(channel)
            return True

        prom = self._modifyClient(clientModification)
        result = yield from prom.result()

        if not result:
            print("Didn't unsubscribe successfully...")

        #Remove duplex channel from list of entities.  Should be thread-safe...
        self.channels.pop(channel, None)

    @asyncio.coroutine
    def wait_for_message(self, channel, timeout=60):
        message = yield from self.channels[channel].async_get(self.loop, timeout=timeout)
        return message

    @asyncio.coroutine
    def send_message(self, channel, message):

        def clientModification():
            self.client.publish(channel, message)
            return True

        prom = self._modifyClient(clientModification)
        result = prom.result()

        if not result:
            print("Didn't send message successfully...")

    def send_message2(self, channel, message):
        def clientModification():
            self.client.publish(channel, message)
            return True

        prom = self._modifyClient(clientModification)
        result = prom.result()

        if not result:
            print("Didn't send message successfully...")

    def run_pipeline(self, pipeline, exeption_handler=None):
        self.loop.set_exception_handler(exeption_handler)
        return self.loop.run_until_complete(pipeline)

    def run_pipeline_async(self, pipieline, exception_handler=None):
        self.loop.set_exception_handler(exception_handler)

    def start(self):

        # Starts MQTT client in background thread
        self.client.loop_start()

        #Loop to handle modifications to client
        def clientQ():
            while True:

                result = self.clientQueue.get()

                if result is None:
                    break

                #Else if not poison-pilled
                try:
                    result.promise.fulfill(result.f())
                except Exception as e:
                    print("Encountered exception " + repr(e) + " in clientQ")
                    result.promise.fulfill(e)


            return True

        self.futures.append(self.executor.submit(clientQ))


    def stop(self):
        #Stops MQTT client
        self.client.loop_stop()
        self.clientQueue.put(None)

        results = [f.result(timeout=5) for f in self.futures]

        if not any(results):
            #If any of the results are NOT true
            print("Encountered error handling a future.  You should inspect the futures array to ensure everything is functioning")


        self.executor.shutdown()
