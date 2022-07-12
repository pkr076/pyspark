
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Map Demo").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    ipRdd = sc.textFile('word_count.text')
    opRdd = ipRdd.flatMap(lambda x: x.split(' '))
    opRdd.saveAsTextFile("output")