from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("foldByKey Demo").setMaster('local')
    sc = SparkContext(conf=conf)

    lst = [("prashant", "PKR"),("shivesh","SKS"),("sanjay","DOC"), ("prashant", "192"),("shivesh","819"),("sanjay","610")]
    rdd1 = sc.parallelize(lst)
    print(type(rdd1))
    rdd2 = rdd1.foldByKey("", lambda s1,s2 : s1+s2)
    lst2 = rdd2.collect()
    print(lst2)
