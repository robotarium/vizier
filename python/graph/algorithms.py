#Algorithms module
import graph
#This is a file containing graph algorithms.  Graphs are expected in the form of an adjacency list (dict) with keys as strings

def kahnSort(graph):
    #Ensure that we won't change the original graph
    graph_ = graph.clone()
    toSort = list()
    done = list()

    for v in graph_.graph:
        if(graph_.inDegree(v) == 0):
            toSort.append(v)

    while(len(to_sort) > 0):
        current = toSort.pop()
        done.append(current)

        for v in set(graph_.graph[current]['out']):
            graph_.removeEdge(v, current)
            if(graph_.inDegree(v) == 0):
                toSort.append(v)

    return done

def kahnSortDepth(graph):
    #Ensure that we won't change the original graph
    graph_ = graph.clone()
    toSort = list()
    depth = 0
    depthMap = {}

    for v in graph_.graph:
        if(graph_.inDegree(v) == 0):
            toSort.append(v)

    while(len(toSort) > 0):

        depth = depth + 1

        depthMap[depth] = toSort;

        toSortNew = list()

        for u in toSort:
            for v in set(graph_.graph[u]['out']):
                graph_.removeEdge(v, u)
                if(graph_.inDegree(v) == 0):
                    toSortNew.append(v)

        toSort = toSortNew

    return depthMap
