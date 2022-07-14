
from pyspark import SparkContext,SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("subtractByKey Demo").setMaster('local')
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([("pkr", 7),("sks", 7),("akr", 7),("rkr", 7)])
    rdd2 = sc.parallelize([("pkr", "rai"), ("rkr", "info"), ("proj", "7")])
    rdd3 = rdd1.subtractByKey(rdd2)
    print(rdd3.collect())