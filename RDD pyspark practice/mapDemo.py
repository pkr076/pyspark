
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Map Demo").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    ipRdd = sc.textFile("surname.txt")
    mpRdd = ipRdd.map(lambda x:len(x))
    mpRdd.saveAsTextFile("output")