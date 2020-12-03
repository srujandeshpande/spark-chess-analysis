# Program for finding games with most moves 

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


sc = SparkContext("local","Counting number of moves per chess game")

text_file = sc.textFile("hdfs://chess2-m/user/hdfs/data.txt")

table2 = text_file.filter(lambda x: "#" not in x.split(' ')[0])
table3 = table2.filter(lambda x: "blen_false" in x.split(' ')[15])

def game_map(lines):
    lineArr = lines.split(" ")
    num_moves = len(lineArr) - 16
    return (num_moves, 1)

sortedRDD = table2.map(game_map).reduceByKey(lambda a,b: a+b)

sortedRDD.saveAsTextFile("hdfs://chess2-m/user/hdfs/out2")

topTenTuples = list(sortedRDD.takeOrdered(50, key = lambda x: -x[1]))
topTenRDD = sc.parallelize(topTenTuples)

topTenRDD.saveAsTextFile("hdfs://chess2-m/user/hdfs/out")

sc.stop()
