# Program for finding years with most games 

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


sc = SparkContext("local","Counting number of chess games per year")

text_file = sc.textFile("hdfs://chess2-m/user/hdfs/data.txt")

table2 = text_file.filter(lambda x: "#" not in x.split(' ')[0])

def year_map(lines):
    lineArr = lines.split(" ")
    datestr = lineArr[1]
    year = datestr.split('.')[0]
    return (year, 1)

sortedRDD = table2.map(year_map).reduceByKey(lambda a,b: a+b)

sortedRDD.saveAsTextFile("hdfs://chess2-m/user/hdfs/out2")

topTenTuples = list(sortedRDD.takeOrdered(50, key = lambda x: -x[1]))
topTenRDD = sc.parallelize(topTenTuples)

topTenRDD.saveAsTextFile("hdfs://chess2-m/user/hdfs/out")

sc.stop()
