import pyspark
from pyspark.sql.functions import lit, collect_list, col
import sys

spark = pyspark.sql.SparkSession.builder.appName("PageRank").getOrCreate()
from graphframes.examples import Graphs
from graphframes import *

# read in arguments
input_file = sys.argv[1]
output_file = sys.argv[2]

# All edges in the graph based on relationships in dataset
edgeDF = spark.read.csv(input_file, comment="#", sep=r'\t').toDF("src", "dst")
# Create Vertex dataframe that shows all vertices in the graph along with respective rank
# Use vertices and edges to create graph g
vertex_ranks = edgeDF.select('src').union(edgeDF.select('dst')).distinct().withColumnRenamed('src','id').withColumn("rank", lit(1))
g = GraphFrame(vertex_ranks,edgeDF)
iterations = 10

for i in range(iterations):
    contribs = g.outDegrees.filter('outDegree != 0').join(vertex_ranks,['id']).withColumn('contribution', col('rank')/col('outDegree'))
    contribs_dst = edgeDF.join(contribs, edgeDF.src==contribs.id).select('dst','contribution').groupBy('dst').sum().withColumn('newRank',0.15 + 0.85 * col('sum(contribution)'))
    vertex_ranks = vertex_ranks.join(contribs_dst, vertex_ranks.id==contribs_dst.dst, 'leftouter').na.fill(0.15).select('id','newRank').withColumnRenamed('newRank','rank')

vertex_ranks.sort("rank", "id").coalesce(1).write.option("header", True).csv(output_file)

spark.stop()
    


