#Basic module

#Very basic graph functions.

import copy

class Graph:
    """
    test
    """

    def __init__(self):
        self.graph = {}

    @staticmethod
    def emptyVertex():
        """
        Returns the key for an empty vertex
        """
        return {'in' : set(), 'out' : set()}

    def clone(self):
        """
        Creates and returns a deep copy of the graph
        """
        g = copy.deepcopy(self)
        return g

    def addVertex(self, v):
        """
        Adds an empty vertex to the graph
        """
        if(v not in self.graph):
            self.graph[v] = Graph.emptyVertex()

        return self

    def addIncomingEdge(self, v, e):
        """
        Adds an incoming edge from v -> e and an outgoing edge
        from e -> v
        """
        if (v in self.graph):
            self.graph[v]['in'].add(e)
        else:
            self.graph[v] = Graph.emptyVertex()
            self.addIncomingEdge(v, e)

        self.add_outgoing_edge_(e, v)

        return self

    def add_incoming_edge_(self, v, e):
        """
        Adds an incoming edge from v -> e without calling
        add_outgoing_edge(e, v)
        """
        if (v in self.graph):
            self.graph[v]['in'].add(e)
        else:
            self.graph[v] = Graph.emptyVertex()
            self.addIncomingEdge(v, e)

        return self

    def addOutgoingEdge(self, v, e):
        """
        Adds an outgoing edge from v -> e and an incoming
        edge from e -> v
        """
        if(v in self.graph):
            self.graph[v]['out'].add(e)
        else:
            self.graph[v] = Graph.emptyVertex()
            self.addOutgoingEdge(v, e)

        self.add_incoming_edge_(e, v)

        return self

    def add_outgoing_edge_(self, v, e):
        """
        Adds an outgoing edge from v -> e without calling
        add_incoming_edge(e, v)
        """
        if(v in self.graph):
            self.graph[v]['out'].add(e)
        else:
            self.graph[v] = Graph.emptyVertex()
            self.addOutgoingEdge(v, e)

        self.add_incoming_edge_(e, v)

        return self

    def addEdge(self, v, e):
        """
        Adds an incoming and outgoing edge from v -> e
        """
        return (self
                .addIncomingEdge(v, e)
                .addOutgoingEdge(v, e))

    def removeIncomingEdge(self, v, e):
        """
        Removes an incoming edge from v -> e and an
        outgoing edge from e -> v
        """
        if(v in self.graph):
            edges = self.graph[v]['in']
            if(e in edges):
                edges.remove(e)
                self.remove_outgoing_edge_(e, v)

        return self

    def remove_incoming_edge_(self, v, e):
        """
        Removes an incoming edge from v -> e
        """
        if(v in self.graph):
            edges = self.graph[v]['in']
            if(e in edges):
                edges.remove(e)

        return self

    def removeOutgoingEdge(self, v, e):
        """
        Removes an outgoing edge from v -> e and an
        incoming edge from e -> v
        """
        if(v in self.graph):
            edges = self.graph[v]['out']
            if(e in edges):
                edges.remove(e)
                self.remove_incoming_edge_(e, v)

        return self

    def remove_outgoing_edge_(self, v, e):
        """
        Removes an outgoing edge from v -> e
        """
        if(v in self.graph):
            edges = self.graph[v]['out']
            if(e in edges):
                edges.remove(e)

        return self

    def removeEdge(self, v, e):
        """
        Removes an outgoing edge from v -> e and an incoming
        edge from v -> e
        """
        return (self
                .removeOutgoingEdge(v, e)
                .removeIncomingEdge(v, e))

    def inDegree(self, v):
        """
        Returns the in-degree of a graph.  Returns 0 when
        the node is not present
        """
        if(v in self.graph):
            return len(self.graph[v]['in'])
        else:
            return 0

    def outDegree(self, v):
        """
        Returns the out-degree of a graph.  Returns 0 when the
        node is not present
        """
        if(v in self.graph):
            return len(self.graph[v]['out'])
        else:
            return 0

    def degree(self, v):
        """
        Returns the in-degree plus the out-degree of a given
        vertex
        """
        return self.inDegree(v) + self.outDegree(v)

    def inGraph(self, v):
        return (v in self.graph)
