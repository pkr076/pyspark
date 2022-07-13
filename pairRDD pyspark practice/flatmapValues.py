
from pyspark import SparkContext,SparkConf

if __name__ =="__main__":
    conf = SparkConf().setAppName('flatMapValues Demo').setMaster("local")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([("pkr",3),("skr",8),("akr",6),("sks",7)])
    rdd2 = rdd1.mapValues(lambda x: [x,x+1,x+2])
    print(rdd2.collect())

    rdd3 = rdd2.flatMapValues(lambda x:x)
    print(rdd3.collect())