from __future__ import print_function
import sys
from pyspark.sql import SparkSession    

alpha = 0.85
N = 4847571

def computeDeposit(x):
    edge, (listOutEdges, pagerank) = x
    if listOutEdges is not None and len(listOutEdges) != 0:
        deposit = float(pagerank) / len(listOutEdges)
        return [(outEdge ,deposit) for outEdge in listOutEdges]
    else:
        return [(edge, 0)]
def computePagerank(x):
    edge, (listOutEdges, listInPagerank) = x
    if listInPagerank is None:
        pagerank = (1 - alpha) / N
    else:
        pagerank = alpha * sum(listInPagerank) + (1 - alpha) / N
    return (edge, (listOutEdges, pagerank))


if __name__ == "__main__":
	if len(sys.argv) != 3:
	    print("args:  <file>, <iterations>")
	    exit(-1)
	spark = SparkSession.builder.appName("Pagerank").getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	edges = lines.filter(lambda x: len(x) > 0 and x[0] != '#').map(lambda x: x.strip().split('\t'))
	listOutEdges = edges.groupByKey().cache()

	firstGraphAndPagerank = listOutEdges.map(lambda x: (x[0], (x[1], 1.0 / N)))
	graphAndPagerank = firstGraphAndPagerank
  
	for i in range(int(sys.argv[2])):
	    deposits = graphAndPagerank.flatMap(computeDeposit).groupByKey() 
	    graphAndPagerank = listOutEdges.fullOuterJoin(deposits).map(computePagerank)
	
        ranks = graphAndPagerank.map(lambda x: (x[1][1], x[0])).sortBy(lambda x:-x[0])
	top10 = ranks.take(10)
	for vertex, rank in top10:
	    print(vertex, ": ", rank)

	spark.stop()
