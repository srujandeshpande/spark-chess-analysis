# Program for finding upset rate 

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


sc = SparkContext("local","Finding upset rate")

text_file = sc.textFile("hdfs://chess3-m/user/hdfs/data.txt")

for gap in range(100, 500, 50):

    table2 = text_file.filter(lambda x: "#" not in x.split(' ')[0])
    table3 = table2.filter(lambda x: "welo_false" in x.split(' ')[8])
    table4 = table3.filter(lambda x: "belo_false" in x.split(' ')[9])
    table5 = table4.filter(lambda x: len(x.split(' ')[2]) == 3)
    table6 = table5.filter(lambda x: (gap <= abs(int(x.split(' ')[3]) - int(x.split(' ')[4])) < gap+50))

    def game_map(lines):
        lineArr = lines.split(" ")
        if(lineArr[2] == '1-0' and int(lineArr[3])<int(lineArr[4])):
            return ("Upset", 1)
        else:
            return ("Not Upset", 1)

        if(lineArr[2] == '0-1' and int(lineArr[3])>int(lineArr[4])):
            return ("Upset", 1)
        else:
            return ("Not Upset", 1)


    outRDD = table6.map(game_map).reduceByKey(lambda a,b: a+b)

    #outRDD.saveAsTextFile("hdfs://chess3-m/user/hdfs/out")

    j = list(outRDD.take(2))

    p = (int(j[0][1])/int(j[1][1]))*100
    print(f'Upset Rate for Range {gap} to {gap+50} is: {p}%')

sc.stop()
