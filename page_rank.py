import pyspark
from pyspark.sql.functions import lit, coalesce, sum
import sys

from graphframes import GraphFrame
from graphframes.lib import Pregel

# create SparkSession
spark = pyspark.sql.SparkSession.builder.appName("PageRank").getOrCreate()
# provide location of checkpoint directory in HDFS
spark.sparkContext.setCheckpointDir('hdfs://10.10.1.1:9000/checkpoint')

# read in arguments - directory where input dataset is located and where to write output
input_file = sys.argv[1]
output_file = sys.argv[2]

# All edges in the graph based on relationships in dataset
edgeDF = spark.read.csv(input_file, comment="#", sep=r'\t').toDF("src", "dst")
# Create Vertex dataframe that shows all vertices in the graph along with respective out-degree
vertex_DF = edgeDF.select('src').union(edgeDF.select('dst')).distinct().withColumnRenamed('src','id')
vertices = GraphFrame(vertex_DF,edgeDF).outDegrees
# Use vertices and edges to create graph g
g = GraphFrame(vertices,edgeDF)

# receive output dataframe after algorithm has run for 10 iterations
# specify "rank" as column to update with initial values of 1 and update according to formula
# use coalesce to substitute 0 for null values (vertices with no contributions)
# calculate contributions from rank / outDegree and sum to get total contributions
ranks = g.pregel \
     .setMaxIter(10) \
     .withVertexColumn("rank", lit(1.0), \
         coalesce(Pregel.msg(), lit(0.0)) * lit(0.85) + lit(0.15)) \
     .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree")) \
     .aggMsgs(sum(Pregel.msg())) \
     .run()

# select output of just user IDs and ranks and sort accordingly before writing to output location
ranks.select('id','rank').sort("rank", "id").coalesce(1).write.option("header", True).csv(output_file)

spark.stop()