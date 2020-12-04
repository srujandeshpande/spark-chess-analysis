# Program for finding years with most games

# Import statements
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Creating a spark context
sc = SparkContext("local", "Counting number of chess games per year")

# Reading the dataset from HDFS
text_file = sc.textFile("hdfs://chess2-m/user/hdfs/data.txt")

# Filtering to remove comments from the dataset
table2 = text_file.filter(lambda x: "#" not in x.split(" ")[0])


def year_map(lines):
    # First split each line at a space
    lineArr = lines.split(" ")
    # Then Take the 2nd element which is the date
    datestr = lineArr[1]
    # Extract the year from the date
    year = datestr.split(".")[0]
    # Return the year and count
    return (year, 1)


# Reducer to sum the count by year
sortedRDD = table2.map(year_map).reduceByKey(lambda a, b: a + b)

# Store the entire processed data in HDFS
sortedRDD.saveAsTextFile("hdfs://chess2-m/user/hdfs/out2")

# Extract the top 50 years
topTenTuples = list(sortedRDD.takeOrdered(50, key=lambda x: -x[1]))
topTenRDD = sc.parallelize(topTenTuples)

# Store the top 50 years in HDFS
topTenRDD.saveAsTextFile("hdfs://chess2-m/user/hdfs/out")

sc.stop()
