# Link b/w rating and opening move

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


sc = SparkContext("local","Popular opening")

text_file = sc.textFile("hdfs://chess3-m/user/hdfs/data.txt")

table2 = text_file.filter(lambda x: "#" not in x.split(' ')[0])
table3 = table2.filter(lambda x: "blen_false" in x.split(' ')[15])
table4 = table3.filter(lambda x: "welo_false" in x.split(' ')[8])
table5 = table4.filter(lambda x: int(x.split(' ')[3]) > 2400)
table6 = table5.filter(lambda x: x.split(' ')[17][0:3] == "W1.")


def open_map(lines):
    lineArr = lines.split(" ")
    return (lineArr[17], 1)

sortedRDD = table6.map(open_map).reduceByKey(lambda a,b: a+b)

sortedRDD.saveAsTextFile("hdfs://chess3-m/user/hdfs/out2")

topTenTuples = list(sortedRDD.takeOrdered(50, key = lambda x: -x[1]))
topTenRDD = sc.parallelize(topTenTuples)

topTenRDD.saveAsTextFile("hdfs://chess3-m/user/hdfs/out")

sc.stop()
