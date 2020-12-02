import sys

from pyspark import SparkContext, SparkConf

sc = SparkContext("local","PySpark Word Count Exmaple")

text_file = sc.textFile("hdfs://chess2-m/user/hdfs/data.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://chess2-m/user/hdfs/out.txt")
