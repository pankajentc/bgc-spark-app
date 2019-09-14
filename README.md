### Instructions:

[Follow this article to find more detailed instructions.](https://nosqlnocry.wordpress.com/2015/02/27/how-to-build-a-spark-fat-jar-in-scala-and-submit-a-job/)

A Simple Spark application that reads below files from given path

name.basics.tsv.gz title.akas.tsv.gz title.basics.tsv.gz title.crew.tsv.gz title.episode.tsv.gz title.principals.tsv.gz title.ratings.tsv.gz

Deployment Guide:

git clone cd spark-example-project mvn clean install Goto the spark installation directory

submit the application in local mode by using below command please put the actual path for application jar and input file

./bin/spark-submit --class com.bgc.SparkApp --master local bgc-spark-project-0.0.1-SNAPSHOT.jar /input/file/path/

If the cluster is running YARN, you can replace "--master local" with "--master yarn".

The result will be displayed on the console.
