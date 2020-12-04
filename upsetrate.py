# Finding the Upset rate for different intervals
# A game is upset when the player with a lower Elo wins against a player with a higher Elo

# Imports
import sys
from pyspark import SparkContext, SparkConf

# Creating a spark context
sc = SparkContext("local", "Calculating upset rate")

# Reading the dataset from HDFS
text_file = sc.textFile("hdfs://chess2-m/user/hdfs/data.txt")

# Computing the rate for different Elo intervals
for gap in range(100, 500, 50):

    # Filtering to remove comments from the dataset
    table2 = text_file.filter(lambda x: "#" not in x.split(" ")[0])
    # Filtering to remove games where the White player's Elo is not given
    table3 = table2.filter(lambda x: "welo_false" in x.split(" ")[8])
    # Filtering to remove games where the Black player's Elo is not given
    table4 = table3.filter(lambda x: "belo_false" in x.split(" ")[9])
    # Filtering to remove games whos result was a draw
    table5 = table4.filter(lambda x: len(x.split(" ")[2]) == 3)
    # Filtering to find games where the Elo difference is within the specified range
    table6 = table5.filter(
        lambda x: (gap <= abs(int(x.split(" ")[3]) - int(x.split(" ")[4])) < gap + 50)
    )

    def game_map(lines):
        # Split each line at space
        lineArr = lines.split(" ")
        # Checking if White has won and Upset occured
        if lineArr[2] == "1-0" and int(lineArr[3]) < int(lineArr[4]):
            return ("Upset", 1)
        # Checking if Black has won and Upset ocurred
        elif lineArr[2] == "0-1" and int(lineArr[3]) > int(lineArr[4]):
            return ("Upset", 1)
        else:
            return ("Not Upset", 1)

    # Reducer sums the count for upset and not upset
    outRDD = table6.map(game_map).reduceByKey(lambda a, b: a + b)

    # Extracting the two values as a list
    j = list(outRDD.take(2))

    # Calculating and printing the upset percentage
    p = (int(j[0][1]) / int(j[1][1])) * 100
    print(f"Upset Rate for Range {gap} to {gap+50} is: {p}%")

sc.stop()
