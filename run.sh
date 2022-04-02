# This command will pull the necessary package for GraphFrame using spark-submit and run our program with the paths to the web graph dataset and subsequent output
../spark-3.2.1-bin-hadoop3.2/bin/spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 page_rank.py hdfs://10.10.1.1:9000/web-BerkStan.txt output
