sudo apt update
sudo apt install python3-pip
pip3 install kaggle
export PATH="/home/srujan_deshpande/.local/bin:$PATH"

get kaggle.json from website

upload to ~./kaggle

chmod 600 kaggle.json

kaggle datasets download milesh1/35-million-chess-games

sudo apt install unzip


srujan_deshpande@chess2-m:~$ cat wordcount.py
import sys

from pyspark import SparkContext, SparkConf

sc = SparkContext("local","PySpark Word Count Exmaple")

text_file = sc.textFile("hdfs://chess2-m/user/hdfs/test.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://chess2-m/user/hdfs/out.txt")


srujan_deshpande@chess2-m:~$ hdfs dfs -cat /user/hdfs/out.txt/part*
(u'load', 1)
(u'', 1)
(u'lets', 1)
(u'are', 1)
(u'world', 1)
(u'you', 1)
(u'data', 1)
(u'hello', 1)
(u'nice', 1)


hdfs dfs -put test.txt /user/hdfs/test

hdfs dfs -rm -r /user/hdfs/out

spark-submit pp.py 
