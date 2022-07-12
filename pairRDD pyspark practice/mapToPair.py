
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    conf = SparkConf().setAppName("mapToPair Demo").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile('names.txt')
    pair_rdd = rdd1.map(lambda x: (x, len(x)))
    pair_rdd.saveAsTextFile("output")