# Program for finding the most common game length

# Import statements
import sys
from pyspark import SparkContext, SparkConf

# Creating a spark context
sc = SparkContext("local", "Finding most common game length")

# Reading the dataset from HDFS
text_file = sc.textFile("hdfs://chess2-m/user/hdfs/data.txt")

# Filtering to remove comments from the dataset
table2 = text_file.filter(lambda x: "#" not in x.split(" ")[0])
# Filtering to remove games with no specified length
table3 = table2.filter(lambda x: "blen_false" in x.split(" ")[15])


def game_map(lines):
    # First split each line at a space
    lineArr = lines.split(" ")
    # Calculate game length by taking total length minus length of metadata
    num_moves = len(lineArr) - 16
    # Return the number of moves and count
    return (num_moves, 1)


# Reducer the sum the moves count by game
sortedRDD = table3.map(game_map).reduceByKey(lambda a, b: a + b)

# Store the entire processed data in HDFS
sortedRDD.saveAsTextFile("hdfs://chess2-m/user/hdfs/out2")

# Extract the top 50 moves numbers
topTenTuples = list(sortedRDD.takeOrdered(50, key=lambda x: -x[1]))
topTenRDD = sc.parallelize(topTenTuples)

# Store the top 50 in HDFS
topTenRDD.saveAsTextFile("hdfs://chess2-m/user/hdfs/out")

sc.stop()
