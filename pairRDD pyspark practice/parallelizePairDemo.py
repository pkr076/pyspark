
from pyspark import SparkContext,SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("parallelizePair").setMaster("local")
    sc = SparkContext(conf=conf)

    lst = [("prashant",8),("shivesh",7),("aparajita",9)]
    pair_rdd1 = sc.parallelize(lst)
    lst2 = pair_rdd1.collect()
    print(lst2)