import queue
import concurrent.futures
import graph.graph as graph
import mqtt_interface.promise as promise
import weakref

"""
EXPERIMENTAL IMPLEMENTATION OF SOME STUFF.  NOT CURRENTLY IN USE
"""

def identity(x):
    return x

def compose(a, b):
    def f(*args, **kwargs):
        return a(b(*args, **kwargs))
    return f

#Future takes from functional streams should return functional promises

def lift(f):

    local_promise = promise.Promise(executor=global_functional_stream_system.executor)

    def local_lift_f():
        local_promise.fulfill(f())

    global_functional_stream_system.executor.submit(local_lift_f)

    f_promise = FunctionalPromise(local_promise)

    return f_promise

class FunctionalPromise:

    def __init__(self, current_promise=None):

        if(current_promise is None):
            # "Empty" FP is just an FP containing an empty vector
            self.current_promise = promise.Promise(executor=global_functional_stream_system.executor)
            self.current_promise.fulfill([])
        else:
            self.current_promise = current_promise

    # w a -> a
    def extract(self):
        print("extracted")
        return self.current_promise.result().pop()

    #(w a -> b) -> w a -> w b
    #Takes a function on a promise and a promise
    #But mine is really w a -> (w a -> b) -> wb

    def extend(self, f):

        new_promise = promise.Promise(executor=global_functional_stream_system.executor)

        def local_extend_f():
            new_promise.fulfill([f(self)])

        global_functional_stream_system.executor.submit(local_extend_f)

        return FunctionalPromise(new_promise)

    # w a -> (a -> b) -> w b
    def fmap(self, f):

        def unwrap(w):
            f(*w.extract())

        #TODO: Ensure this is compatible with everything else
        return self.extend(unwrap)

class FunctionalStream:

    def __init__(self, function=identity):
        self.queue = queue.Queue()
        self.weak_ref = weakref.ref(self.queue)

        # f : A -> A
        self.function = function
        global_functional_stream_system.queues[self.key()] = self

    def key(self):
        #TODO: I can probably just return this directly
        return repr(id(self.weak_ref()))

    def concat(self, to_f_stream):
        #Needs to take a stream, cat it to another stream.
        # Should put all values from one stream into the next stream
        # If catted stream is empty, just becomes a stream "clone" f.a.i.a.p.

        def local_concat_f():
            msg = self.extract()
            while(msg is not None):
                to_f_stream.of(msg)
                msg = self.extract()
            print("concat completed for " + self.key())

        to_f_stream.of(None)

        return to_f_stream

    def of(self, msg):
        self.queue.put(self.function(msg))

    def extract(self):
        #Rather than destructively taking from a queue, this method should go into a pending
        return self.queue.get()

    def extend(self, f):
        #Needs to return a new stream with all the same connections that operates with a new function
        return FunctionalStream(function = f)

    def map(self, f):
        to_f_stream = FunctionalStream()

        def local_map_f():
            msg = self.extract()
            while(msg is not None):
                to_f_stream.of(f(msg))
                msg = self.extract()

            to_f_stream.of(None)
            print("map completed for " + self.key())

        global_functional_stream_system.executor.submit(local_map_f)

        return to_f_stream

    def chain(self, f):
        to_f_stream = FunctionalStream()

        def local_chain_f():
            msg = self.extract()
            while(msg is not None):
                result = f(msg)
                if(type(result) == list):
                    for r in result:
                        to_f_stream.of(r)
                else:
                    to_f_stream.of(result)
                msg = self.extract()

            to_f_stream.of(None)
            print("chain completed for " + self.key())

        global_functional_stream_system.executor.submit(local_chain_f)

        return to_f_stream

    def reduce(self, f, init):
        pass

    def close(self):
        self.of(None)

class FunctionalStreamSystem:

    def __init__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=128)
        self.system_graph = graph.Graph()
        self.futures = {}
        self.queues = {}

    def add_stream(self, f, from_stream, to_stream):

        #Make sure we do this first so that we get a valid weak reference
        from_key = from_key.key()
        to_key = to_stream.key()
        self.system_graph.addVertex(to_key)
        self.system_graph.addOutgoingEdge(from_key, to_key)

        # future = self.executor.submit(f)
        # self.futures[to_key] = future

    def remove_stream(self, stream):
        pass

# Begin function definitions
global_functional_stream_system = FunctionalStreamSystem()
