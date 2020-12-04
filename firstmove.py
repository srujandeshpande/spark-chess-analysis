# Finding the most common starting moves for players with elo > 2400

# Imports
import sys
from pyspark import SparkContext, SparkConf

# Creating a spark context
sc = SparkContext("local", "Finding most common starting moves")

# Reading the dataset from HDFS
text_file = sc.textFile("hdfs://chess2-m/user/hdfs/data.txt")

# Filtering to remove comments from the dataset
table2 = text_file.filter(lambda x: "#" not in x.split(" ")[0])
# Filtering to remove games with no specified length
table3 = table2.filter(lambda x: "blen_false" in x.split(" ")[15])
# Filtering to remove games where the White player's Elo is not given
table4 = table3.filter(lambda x: "welo_false" in x.split(" ")[8])
# Filtering to remove games where the player has elo less that 2400
table5 = table4.filter(lambda x: int(x.split(" ")[3]) > 2400)
# Filterint to remove games that do not have the first move recorded
table6 = table5.filter(lambda x: x.split(" ")[17][0:3] == "W1.")


def open_map(lines):
    # Split each line at a space
    lineArr = lines.split(" ")
    # Return the first move and the count
    return (lineArr[17], 1)


# Reducer sums the count by first move
sortedRDD = table6.map(open_map).reduceByKey(lambda a, b: a + b)

# Store the entire processed data in HDFS
sortedRDD.saveAsTextFile("hdfs://chess3-m/user/hdfs/out2")

# Extract the top 50 moves
topTenTuples = list(sortedRDD.takeOrdered(50, key=lambda x: -x[1]))
topTenRDD = sc.parallelize(topTenTuples)

# Store the top 50 moves in HDFS
topTenRDD.saveAsTextFile("hdfs://chess3-m/user/hdfs/out")

sc.stop()
