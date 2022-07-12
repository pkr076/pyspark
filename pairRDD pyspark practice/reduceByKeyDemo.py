from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("reduceByKey Demo").setMaster('local')
    sc = SparkContext(conf=conf)

    lst = [("prashant", 492),("shivesh",489),("sanjay",460), ("prashant", 192),("shivesh",819),("sanjay",610)]
    rdd1 = sc.parallelize(lst)
    rbk = rdd1.reduceByKey(lambda x,y: x if x<y else y)
    lst2 = rbk.collect()
    print(lst2)