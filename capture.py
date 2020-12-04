# Finding if more captures results in a win

# Imports
import sys
from pyspark import SparkContext, SparkConf

# Creating a spark context
sc = SparkContext("local", "More captures to win")

# Reading the dataset from HDFS
text_file = sc.textFile("hdfs://chess3-m/user/hdfs/data.txt")

# Filtering to remove comments from the dataset
table2 = text_file.filter(lambda x: "#" not in x.split(" ")[0])
# Filtering to remove games with no specified length
table3 = table2.filter(lambda x: "blen_false" in x.split(" ")[15])
# Filtering to remove games whos result was a draw
table4 = table3.filter(lambda x: len(x.split(" ")[2]) == 3)


def game_map(lines):
    # Split each line at space
    lineArr = lines.split(" ")
    # Initialize number of captures to 0
    w = 0
    b = 0
    # For each move, check if there was a capture. If captured, increment the player capture count
    for i in lineArr[17:]:
        if "x" in i:
            if "W" in i:
                w += 1
            else:
                b += 1
    # Return output based on win or lose compared to number of captures
    if lineArr[2] == "1-0" and w > b:
        return ("More captures", 1)
    elif lineArr[2] == "0-1" and b > w:
        return ("More captures", 1)
    else:
        return ("Less captures", 1)


# Reducer sums the count by more or lesscaptures
outRDD = table4.map(game_map).reduceByKey(lambda a, b: a + b)

# Convert RDD to list and print
j = list(outRDD.take(2))
print(j)

sc.stop()
