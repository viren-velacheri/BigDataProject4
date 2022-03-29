import pyspark
from pyspark.sql.functions import lit, collect_list, col, coalesce, col, lit, sum, when
import sys

spark = pyspark.sql.SparkSession.builder.appName("PageRank").getOrCreate()
spark.sparkContext.setCheckpointDir('hdfs://10.10.1.1:9000/checkpoint')
from graphframes.examples import Graphs
from graphframes import *
from graphframes.lib import Pregel

# read in arguments
input_file = sys.argv[1]
output_file = sys.argv[2]

# All edges in the graph based on relationships in dataset
edgeDF = spark.read.csv(input_file, comment="#", sep=r'\t').toDF("src", "dst")
# Create Vertex dataframe that shows all vertices in the graph along with respective rank
# Use vertices and edges to create graph g
vertex_DF = edgeDF.select('src').union(edgeDF.select('dst')).distinct().withColumnRenamed('src','id')
vertices = GraphFrame(vertex_DF,edgeDF).outDegrees
g = GraphFrame(vertices,edgeDF)

ranks = g.pregel \
     .setMaxIter(10) \
     .withVertexColumn("rank", lit(1.0), \
         coalesce(Pregel.msg(), lit(0.0)) * lit(0.85) + lit(0.15)) \
     .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree")) \
     .aggMsgs(sum(Pregel.msg())) \
     .run()

#for i in range(iterations):
#    contribs = g.outDegrees.filter('outDegree != 0').join(vertex_ranks,['id']).withColumn('contribution', col('rank')/col('outDegree'))
#    contribs_dst = edgeDF.join(contribs, edgeDF.src==contribs.id).select('dst','contribution').groupBy('dst').sum().withColumn('newRank',0.15 + 0.85 * col('sum(contribution)'))
#    vertex_ranks = vertex_ranks.join(contribs_dst, vertex_ranks.id==contribs_dst.dst, 'leftouter').na.fill(0.15).select('id','newRank').withColumnRenamed('newRank','rank')

ranks.select('id','rank').sort("rank", "id").coalesce(1).write.option("header", True).csv(output_file)

spark.stop()
    


