import mqtt_interface.mqttinterface as mqtt
import pprint
import functools as ft
import json
import collections
from utils import utils
import vizier.node as node
import concurrent.futures as futures

### For logging ###
import logging
import logging.handlers as handlers

# Setting up the desired logging format and debug level
logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# TODO: Vizier -> gets all the node descriptors -> ensures dependencies are met
# TODO: Standardize this to look more like a node that doesn't really connect to anything
# TODO:  Supercedes previous two TODOs.  I think that this really needs to be more of an optional
# thing to use the 'vizier' node.  Forget the security stuff.  This can be handled differently anyway.
# If you know the topic name, just request that topic explicitly.  If you want a wildcard or something, just
# do a request to vizier/?/... (or something) to get the requisite information.
# TODO: Add some sort of query engine to get vizier to give information about the running nodes.  This would
# Actually be a more useful feature, anyway

# This class inherits from base node to reuse some of the functionality
class Vizier(node.Node):

    def __init__(self, host, port, nodes):
        """Initializes the vizier node.  Is able to inspect all nodes that are passed in.
        host: string (MQTT host)
        port: int (MQTT port)
        nodes: list (containing nodes that should be inspected)
        
        -> None"""

        self.nodes = nodes

        # Auto-generate this descriptor based on the nodes
        vizier_descriptor = \
        {   
            "end_point": "vizier",
            "links": {}, 
            "requests": self.nodes 
        }

        super().__init__(host, port, vizier_descriptor) # Same as super(Vizier, self).__init__(...)
        
        # To contain future node descriptors
        self.node_descriptors = []
        self.executor = futures.ThreadPoolExecutor(max_workers=100)

    def start(self, retries=30, timeout=1):

        self.mqtt_client.start()

        request_links = [x + '/node_descriptor' for x in self.nodes]
        results = self.executor.map(lambda x: self._make_request('GET', x, {}, retries=retries, timeout=timeout), request_links)
        expanded_descriptors = [utils.generate_links_from_descriptor(x['body']) for x in results]

        # Build dependency graph for list of links
        self.link_graph = dict({list(x.keys())[0].split('/')[0]: {'requests': y, 'links': x} for x, y in expanded_descriptors})
        self.links = list([y for x in self.link_graph.values() for y in x['links'].keys()])

    def get_links(self):
        return self.links

    def get_deps(self):
        return dict({x: set(y['requests']) for x, y in self.link_graph.items()})

    def get_link_deps(self):
        deps = self.get_deps()
        return dict({x: set([z.split('/')[0] for z in y]) for x, y in deps.items()})

    def listen(self, link):
        if(link in self.links):
            self.mqtt_client.subscribe_with_callback(link, lambda x: print(x))    
        else:
            raise ValueError('Link was not present!')

    def unlisten(self, link):
        if(link in self.links):
            self.mqtt_client.unsubscribe(link)
        else:
            raise ValueError('Link was not present!')

    def get(self, link):
        #TODO: finish
        pass

    def stop(self):
        """Safely shuts down the vizier node.
        
        -> None"""

        self.mqtt_client.stop()
