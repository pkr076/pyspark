
from pyspark import SparkContext,SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("combineByKey").setMaster("local")
    sc = SparkContext(conf=conf)

    lst = [("prashant",31),("kanhaiya",27),("prashant",29),("kanhaiya",31),("xyz",31),("abc",27)]
    rdd1 = sc.parallelize(lst)
    rdd2 = rdd1.keys()
    print(rdd2.collect())
    print(rdd1.values().collect())
    rdd3 = rdd1.sortByKey()
    print(rdd3.collect())