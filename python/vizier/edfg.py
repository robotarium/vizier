import graph.graph as graph
import graph.algorithms as algorithms
import concurrent.futures
import functools as ft
from vizier.colors import *

#TODO: Groups, etc

class EDFGBuilder:

    def __init__(self):
        self.graph = graph.Graph()
        self.applicationTree = None
        self.nodes = {}

    def get_starting_order(self):
        """
        Uses a topological sort to return the starting order
        """
        return algorithms.kahnSortDepth(self.graph)

    def with_node(self, node):
        """
        Adds a node to the node graph
        """
        #Ensure that all dependencies are in the current node graph

        self.graph.addVertex(node.name)

        #Convenient map for accessing nodes
        self.nodes[node.name] = node

        #Add a dependency edge between the current node and the dependency
        for d in node.deps:
            self.graph.addOutgoingEdge(d, node.name)

        return self

    def build(self):
        self.applicationTree = self.get_starting_order()
        return EDFG(self)

class EDFG:

    def __init__(self, applicationBuilder):
        #node (DFG-ish) graph of the system
        self.graph = applicationBuilder.graph
        self.applicationTree = applicationBuilder.applicationTree
        self.nodes = applicationBuilder.nodes

    def execute_async(self):
        """
        Begins execution in a separate thread.  Returns a future containing the result of the operation
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers = 1) as executor:
            return executor.submit(self.execute)

    def execute(self, pretty_print=False):
        """
        Traverses the application tree and begins each task
        """

        futures = {}
        outputs = {}

        #maxConcurrentTasks = max([len(v) for _, v in self.applicationTree.items()])
        maxConcurrentTasks = 1

        #TODO: I should make parallelization optional...
        with concurrent.futures.ThreadPoolExecutor(max_workers = maxConcurrentTasks) as executor:

            #Execute sources (level 1)
            vs = self.applicationTree[1]
            treeDepth = len(self.applicationTree)

            for v in vs:

                if(pretty_print):
                    print("EDFG executing node: " + v, end = " ")

                futures[v] = executor.submit(self.nodes[v].xform)

            outputs = {node: future.result() for (node, future) in futures.items()}

            print(colors.OKGREEN + "[ ok ]" + colors.WHITE)


            for level in range(2, treeDepth+1):

                #Get nodes that we can start on the current level
                vs = self.applicationTree[level]

                for v in vs:

                    if(pretty_print):
                        print((' ' * level) + "@[" + repr(level) + "]: " + "EDFG executing node: " + v, end=" ", flush=True)

                    #Get arguments from previous output
                    args = [outputs[x] for x in self.nodes[v].deps]

                    #Wrap calling function and splaterino args
                    futures[v] = executor.submit(lambda: self.nodes[v].xform(*args))
                    futures[v].result()

                #Wait on all results before continuing execution.  Merge previous results
                outputs.update({node: future.result() for (node, future) in futures.items()})

                if(pretty_print):
                    print(colors.OKGREEN + "[ ok ]" + colors.WHITE)

            #Return only the results from the final layer
            return {node:result for (node, result) in [(x, outputs[x]) for x in self.applicationTree[treeDepth]]}

    def stop(self):

        for level in reversed(range(1, len(self.applicationTree) + 1)):

            vs = self.applicationTree[level]

            for v in vs:
                print("Stoping: " + self.nodes[v].name)
                #Call resolution function early if explicitly cancelled
                self.nodes[v].resolution()
