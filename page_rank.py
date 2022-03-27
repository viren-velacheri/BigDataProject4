import pyspark
from pyspark.sql.functions import lit, collect_list
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
vertex_ranks = edgeDF.select('src').union(edgeDF.select('dst')).distinct().withColumnRenamed('src','id').withColumn("Rank", lit(1))
g = GraphFrame(vertex_ranks,edgeDF)


