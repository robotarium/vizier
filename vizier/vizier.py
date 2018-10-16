import graphviz
import vizier.utils as utils
import vizier.node as node
import concurrent.futures as futures
import argparse
import logging
import time
import json


# TODO: Split up some of these functions into network query vs graph operations
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

        """
        # Auto-generate this descriptor based on the nodes
        vizier_descriptor = {
            "end_point": "vizier",
            "links": {},
            "requests": []
        }

        super().__init__(host, port, vizier_descriptor)

        # To contain future node descriptors.
        self._nodes = nodes

        # These attributes are set in the start method.
        self._nodes_to_descriptors = None
        self._link_graph = None
        self._links = None

    def start(self, attempts=15, timeout=0.25, max_workers=100):
        """Starts the vizier node

        Starts the underlying MQTT client and makes GET requests for specified nodes.  These requests retrieve all the relevant data for the nodes so that
        inspection can occur.  This data is only retrieved once and is static over the lifetime of the vizier object.

        Args:
            retries (int):  Number of times to retry the GET requests
            timeout (double): Timeout for the GET requests

        """
        self._mqtt_client.start()

        request_links = [x + '/node_descriptor' for x in self._nodes]
        # Paralellize GET requests
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(lambda x: self._make_request('GET', x, {}, attempts=attempts, timeout=timeout), request_links))

        # Check that we got all the required node descriptors
        in_error = []
        for i, r in enumerate(results):
            if r is None:
                in_error.append(self._nodes[i])
            else:
                try:
                    results[i] = json.loads(r['body'])
                except Exception as e:
                    in_error.append(self._nodes[i])
                    print(repr(e))
                    results[i] = None

        # If not empty
        if(in_error):
            self._logger.warning('Could not retrieve descriptor for nodes ({}).'.format(in_error))

        self._nodes_to_descriptors = dict({x: y for x, y in zip(self._nodes, results) if y is not None})
        self._expanded_descriptors = dict({x: utils.generate_links_from_descriptor(y) for x, y in self._nodes_to_descriptors.items()})
        self._link_graph = dict({x: {'links': y[0], 'requests': y[1]} for x, y in self._expanded_descriptors.items()})
        self._links = dict({y: z for x in self._link_graph.values() for y, z in x['links'].items()})

    def visualize(self):
        graph = graphviz.Digraph(comment='System Graph')

        # Initialize graph and subgraph
        for x, y in self._link_graph.items():

            name = 'cluster'+x
            subgraph = graphviz.Digraph(name=name)
            subgraph.attr(label=x)
            subgraph.attr(color='blue')
            # Create a dummy node for inter-graph connections
            subgraph.node('dummy_'+x, shape='point', style='invis')

            for z in y['links']:
                subgraph.node(z, label=z+' ('+y['links'][z]['type'].lower()+')', shape='box')  # took out constraint=False

            graph.subgraph(subgraph)

        for x, y in self._link_graph.items():
            for z in y['requests']:
                from_endpoint = z.split('/')[0]
                edge = 'dummy_'+x
                rhead = 'cluster'+x
                color = 'black' if from_endpoint in self._link_graph else 'red'
                label = 'required' if y['requests'][z]['required'] else 'optional'

                if(from_endpoint not in self._link_graph):
                    graph.node(z, color='red', label=z+' (unavailable)')

                graph.edge(z, edge, rhead=rhead, color=color, label=label)  # took out constraint=False

        print(graph.source)
        print('~~ PASTE THE ABOVE INTO GRAPHVIZ SOMEWHERE ~~')

    def verify_deps(self):
        """Verifies the dependencies of the specified nodes.

        Ensures that for each request that has been made, that link is being provided by another provided node.

        Returns:
            True if all the inter-node dependencies are met

        Raises:
            ValueError: If any dependency is unsatified

        """
        # TODO: Modify this to account for new request structure
        # Filter out optional dependencies
        only_required = dict({x: {i for i, j in y['requests'].items() if j['required']} for x, y in self._link_graph.items()})
        unsatisfied = [{'node': x, 'unsatisfied': y - y.intersection(self._links)}
                       for x, y in only_required.items() if not y.issubset(self._links)]

        # Now only look at the optional dependencies and see if any are missing
        only_optional = dict({x: {i for i, j in y['requests'].items() if not j['required']} for x, y in self._link_graph.items()})
        optionally_unsatisfied = list([{'endpoint': x,
                                        'unsatisfied': y - y.intersection(self._links)} for x, y in only_optional.items() if not y.issubset(self._links)])

        return {'unsatisfied': unsatisfied, 'optionally_unsatisfied': optionally_unsatisfied}

    def get_links(self):
        return self._links

    def listen(self, link, callback=print):
        """Listens on a particular link for all information.  Topic must be subscribable (i.e., remote STREAM)

        Args:
            link (str): The link to which the vizier listens
        """

        if(link in self._links):
            if(self._links[link]['type'] != 'STREAM'):
                self._logger.warning('Link ({0}) is not of type STREAM ({1})'.format(link, self._links[link]))
        else:
            self._logger.warning('Link ({}) not listed in retrieved node descriptors.'.format(link))

        self._mqtt_client.subscribe_with_callback(link, callback)

    def unlisten(self, link):

        if(link in self._links):
            if(self._links[link]['type'] != 'STREAM'):
                self._logger.warning('Link ({0}) is not of type STREAM ({1}).'.format(link, self._links[link]))
        else:
            self._logger.warning('Link ({}) not listed in retrieved node descriptors.'.format(link))

        self._mqtt_client.unsubscribe(link)

    def get(self, link, attempts=10, timeout=0.25):

        if(link in self._links):
            if(self._links[link]['type'] != 'DATA'):
                self._logger.warning('Link ({0}) is not of type DATA ({1}).'.format(link, self._links[link]))
        else:
            self._logger.warning('Link ({}) not listed in retrieved node descriptors.'.format(link))

        st = time.time()
        response = self._make_request('GET', link, {}, attempts=attempts, timeout=timeout)
        print(time.time() - st)
        return response

    def stop(self):
        """Safely shuts down the vizier node."""

        super().stop()


# CLI
def main():

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(prog='VIZIER QUERY')
    parser.add_argument('--host', type=str, default='localhost', help='IP for the MQTT broker.')
    parser.add_argument('--port', type=int, default=1884, help='Port for the MQTT broker.')
    parser.add_argument('nodes', nargs='*', type=str, help='List of nodes for the MQTT broker.')
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--get', nargs='+', help='Get data from the list of links.')
    action_group.add_argument('--visualize', action='store_true', help='Visualize the network')
    action_group.add_argument('--listen', nargs='+', help='Listen on the list of links')

    args = parser.parse_args()

    v = Vizier(args.host, args.port, args.nodes)
    try:
        v.start()
    except Exception as e:
        print(type(e), ':', e)
        try:
            v.stop()
        except Exception:
            pass
        return

    if(args.get):
        try:
            for x in args.get:
                print('Got ({0}) from link ({1})'.format(v.get(x), x))

        except Exception as e:
            print(type(e), ':', e)
    elif(args.visualize):
        v.visualize()
    elif(args.listen):
        try:
            for x in args.listen:
                v.listen(x, callback=lambda y: print(x, ': ', y))
        except Exception as e:
            print(type(e), ':', e)

        print('ctrl+c to quit')

        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            for x in args.listen:
                v.unlisten(x)
    else:
        print('No action supplied.')
        parser.print_usage()

    v.stop()


if __name__ == '__main__':
    main()
