## Task 1 Instructions

Please ensure firstly that the web-BerkStan.txt dataset is moved to hdfs prior to execution. Make sure that it is located in the root directory as it will be
referenced as such by our run script. Please also make sure that Spark is located one directory above the current directory with the program files. Lastly, please
create a 'checkpoint' directory in hdfs under the root directory (i.e. such that it is /checkpoint in hdfs). Please clear this directory prior to each run of the
program.

To run the page rank program for task 1, execute the following command:

<b> ./run.sh </b> 

This will execute page rank using graph frames for 10 total iterations and will produce output locally as a csv in a directory called "output" (NOT IN HDFS).
The csv contained within this directory will have information about vertex id's and final ranks.
