import graph
import algorithms

def main():
    g = graph.Graph()
    g.addOutgoingEdge('A', 'B') \
    .addOutgoingEdge('B', 'C') \
    .addOutgoingEdge('D', 'E') \
    .addOutgoingEdge('E', 'F')


    print(algorithms.kahnSortDepth(g))

if __name__ == "__main__":
    main()
