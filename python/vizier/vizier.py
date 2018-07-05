import graphviz
import vizier.utils as utils
import vizier.node as node
import concurrent.futures as futures
import logging

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


class Vizier(node.Node):
    """Handles inspection and dependency verification for the nodes passed into the network

    Attributes:
        host (str): MQTT host to which vizier node connects
        port (int):  MQTT port to which vizier node connects

    """

    def __init__(self, host, port, nodes):
        """Initializes the vizier node.  Is able to inspect all nodes that are passed in.

        Args:
            host (str): MQTT host to which vizier node connects
            port (int):  MQTT port to which vizier node connects
            nodes (list):  List of nodes which the vizier node inspects
        """

        self.nodes = nodes

        # Auto-generate this descriptor based on the nodes
        vizier_descriptor = {
            "end_point": "vizier",
            "links": {},
            "requests": self.nodes
        }

        super().__init__(host, port, vizier_descriptor)

        # To contain future node descriptors
        self.node_descriptors = []
        self.executor = futures.ThreadPoolExecutor(max_workers=100)

    def start(self, retries=30, timeout=1):
        """Starts the vizier node.

        Starts the underlying MQTT client and makes GET requests for specified nodes.  These requests retrieve all the relevant data for the nodes so that
        inspection can occur.  This data is only retrieved once and is static over the lifetime of the vizier object.

        Args:
            retries (int):  Number of times to retry the GET requests
            timeout (double): Timeout for the GET requests
        """

        self.mqtt_client.start()

        # Retrieves and expands all the node descriptors for the requested nodes
        request_links = [x + '/node_descriptor' for x in self.nodes]
        results = self.executor.map(lambda x: self._make_request('GET', x, {}, retries=retries, timeout=timeout), request_links)
        expanded_descriptors = [utils.generate_links_from_descriptor(x['body']) for x in results]

        # Build dependency graph for list of links
        self.link_graph = dict({list(x.keys())[0].split('/')[0]: {'requests': set(y), 'links': set(x)} for x, y in expanded_descriptors})
        self.links = set([y for x in self.link_graph.values() for y in x['links']])

    def verify_deps(self):
        """Verifies the dependencies of the specified nodes.

        Ensures that for each request that has been made, that link is being provided by another provided node.

        Returns:
            True if all the dependencies are met

        Raises:
            ValueError: If any dependency is unsatified

        """

        unsatisfied = [{'node': x, 'unsatisfied': y['requests'] - y['requests'].intersection(self.links)}
                       for x, y in self.link_graph.items() if not y['requests'].issubset(self.links)]
        if unsatisfied:
            raise ValueError('Dependencies {} unsatisfied'.format(unsatisfied))
        else:
            return True

    def visualize(self):
        graph = graphviz.Digraph(comment='System Graph')

        # Initialize graph and subgraph
        for x, y in self.link_graph.items():
            subgraph = graphviz.Digraph(name='cluster'+x)
            # Create a dummy node for inter-graph connections
            subgraph.node('D'+x, shape='point', style='invis')
            for z in y['links']:
                subgraph.node(z, constraint='false')
            graph.subgraph(subgraph)

        for x, y in self.link_graph.items():
            for z in y['requests']:
                graph.edge(z, 'D'+x, constraint='false', rhead='cluster'+x)

        print(graph.source)

        return graph

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
        # TODO: finish
        pass

    def stop(self):
        """Safely shuts down the vizier node.

        -> None"""

        self.mqtt_client.stop()
