import graphviz
import vizier.utils as utils
import vizier.node as node
import concurrent.futures as futures

# TODO: Vizier -> gets all the node descriptors -> ensures dependencies are met
# TODO: Add some sort of query engine to get vizier to give information about the running nodes.  This would
# Actually be a more useful feature, anyway


class Vizier(node.Node):
    """Handles inspection and dependency verification for the nodes passed into the network

    Attributes:
        host (str): MQTT host to which vizier node connects
        port (int):  MQTT port to which vizier node connects

    """

    def __init__(self, host, port, nodes, logger=None):
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

        super().__init__(host, port, vizier_descriptor, logger=logger)

        # To contain future node descriptors
        self.node_descriptors = []
        self.executor = futures.ThreadPoolExecutor(max_workers=100)

    def start(self, retries=30, timeout=1):
        """Starts the vizier node

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
        expanded_descriptors = list([utils.generate_links_from_descriptor(x['body']) for x in results])
        self.network_descriptor = dict({y: z for x, _ in expanded_descriptors for y, z in x.items()})

        # Build dependency graph for list of links
        self.link_graph = dict({list(x.keys())[0].split('/')[0]: {'requests': y, 'links': set(x)} for x, y in expanded_descriptors})
        self.links = set([y for x in self.link_graph.values() for y in x['links']])

    def verify_deps(self):
        """Verifies the dependencies of the specified nodes.

        Ensures that for each request that has been made, that link is being provided by another provided node.

        Returns:
            True if all the dependencies are met

        Raises:
            ValueError: If any dependency is unsatified

        """

        # TODO: Modify this to account for new request structure
        # Filter out optional dependencies
        only_required = {x: {i for i, j in y['requests'].items() if j} for x, y in self.link_graph.items()}
        unsatisfied = [{'node': x, 'unsatisfied': y - y.intersection(self.links)}
                       for x, y in only_required.items() if not y.issubset(self.links)]

        # Now only look at the optional dependencies and see if any are missing
        only_optional = {x: {i for i, j in y['requests'].items() if not j} for x, y in self.link_graph.items()}
        optionally_unsatisfied = [{'node': x, 'unsatisfied': y - y.intersection(self.links)} for x, y in only_optional.items() if not y.issubset(self.links)]

        if optionally_unsatisfied:
            self.logger.info('Optional dependencies {} unsatisfied'.format(optionally_unsatisfied))

        if unsatisfied:
            raise ValueError('Dependencies {} unsatisfied'.format(unsatisfied))
        else:
            return True

    def visualize(self, to_display=False):
        graph = graphviz.Digraph(comment='System Graph')

        # Initialize graph and subgraph
        for x, y in self.link_graph.items():

            name = 'cluster'+x if to_display else x
            subgraph = graphviz.Digraph(name=name)

            # Create a dummy node for inter-graph connections
            if(to_display):
                subgraph.node('D'+x, shape='point', style='invis')

            for z in y['links']:
                subgraph.node(z)  # took out constraint=False
            graph.subgraph(subgraph)

        for x, y in self.link_graph.items():
            for z in y['requests']:
                if(to_display):
                    edge = 'D'+x
                    rhead = 'cluster'+x
                else:
                    edge = x
                    rhead = x
                graph.edge(z, edge, rhead=rhead)  # took out constraint=False

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
        """Listens on a particular link for all information.  Topic must be subscribable (i.e., remote STREAM)

        Args:
            link (str): The link to which the vizier listens
        """

        if(link in self.links):
            # Link should always be present in network descriptor, since self.links is just a set of keys of that dict
            if(self.network_descriptor[link]['type'] == 'STREAM'):
                self.mqtt_client.subscribe_with_callback(link, lambda x: print(x))
            else:
                raise ValueError('Link is not type stream ({})'.format(self.network_descriptor[link]))
        else:
            raise ValueError('Link was not present!')

    def unlisten(self, link):

        if(link in self.links):
            if(self.network_descriptor[link]['type'] == 'STREAM'):
                self.mqtt_client.unsubscribe(link)
            else:
                raise ValueError('Link is not type STREAM ({})'.format(self.network_descriptor[link]))
        else:
            raise ValueError('Link was not present!')

    def get(self, link):
        # TODO: finish
        pass

    def stop(self):
        """Safely shuts down the vizier node."""

        super().stop()
        self.executor.shutdown()
