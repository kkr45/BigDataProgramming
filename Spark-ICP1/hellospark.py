import os

os.environ["SPARK_HOME"] = "/Users/kranthikiranreddy/spark-2.2.0-bin-hadoop2.7"
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile("word.txt", 1)
    counts = lines.flatMap(lambda x: x.split(',')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    sc.stop()