
from pyspark import SparkContext,SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("subtractByKey Demo").setMaster('local')
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('k1', 2), ('k3', 4), ('k3', 6)])
    rdd2 = sc.parallelize([('k3', 8), ('k3', 7)])
    rdd3 = rdd1.join(rdd2)
    print(rdd3.collect())

    temp =  [(x, tuple(map(list, y))) for x, y in sorted(list(rdd1.cogroup(rdd2).collect()))]
    #rdd4 = rdd1.cogroup(rdd2)
    #print(rdd4.mapValues(list).collect())
    print(temp)