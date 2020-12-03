# Program for finding upset rate 

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


sc = SparkContext("local","Finding upset rate")

text_file = sc.textFile("hdfs://chess3-m/user/hdfs/data.txt")

table2 = text_file.filter(lambda x: "#" not in x.split(' ')[0])
table3 = table2.filter(lambda x: "blen_false" in x.split(' ')[15])
table4 = table3.filter(lambda x: len(x.split(' ')[2]) == 3)

def game_map(lines):
    lineArr = lines.split(" ")
    w = 0
    b = 0
    for i in lineArr[17:]:
        if 'x' in i:
            if 'W' in i:
                w+=1
            else:
                b+=1

    if(lineArr[2] == "1-0" and w>b):
        return ("More captures", 1)
    elif(lineArr[2] == "0-1" and b>w):
        return ("More captures", 1)
    else:
        return ("Less captures", 1)

    return (num_moves, 1)


outRDD = table4.map(game_map).reduceByKey(lambda a,b: a+b)

#outRDD.saveAsTextFile("hdfs://chess3-m/user/hdfs/out")

j = list(outRDD.take(2))
print(j)

sc.stop()
