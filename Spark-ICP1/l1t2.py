import os
os.environ["SPARK_HOME"] = '/Users/kranthikiranreddy/spark-2.2.0-bin-hadoop2.7'
from operator import add
def hashp(x):
    return hash(x)


from pyspark import SparkContext
from pyspark import *


if __name__ == "__main__":

    sc = SparkContext(appName="secCount")
    lines = sc.textFile("l1t21.txt", 1)
    #counts = lines.flatMap(lambda x: x.split(',')).map(lambda x: (x, 1)).reduceByKey(add)
    #output = counts.collect()
    #for (word, count) in output:
     #   print("%s, %i" % (word, count))
    tokens = lines.flatMap(lambda x:x.split('\n'))
    print(tokens.collect())
    r=tokens.collect()[0].split(',')[0]
    li=tokens.map(lambda x:x.split(','))

    pairs=li.map(lambda x:(x[0]+'-'+x[1],x[3]))
    pairs=pairs.partitionBy(4,hashp)
    print(pairs.getNumPartitions())
    pairs1=pairs.groupByKey()
    print(pairs1)

    q=[]
    for x,y in pairs1.collect():
        for m in y:
            q.append(int(m))
            j=sorted(q,reverse=True)

        print(str(x),j)




