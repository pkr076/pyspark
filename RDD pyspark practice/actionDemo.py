
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Distinct Demo").setMaster("local[3]")
    context = SparkContext(conf=conf)

    lst = [1,2,3,3,5,6,9,5,4,6]
    rdd1 = context.parallelize(lst)
    # rdd2 = rdd1.distinct()
    # lst2 = rdd2.collect()
    # print(lst2)
    # print(rdd1.count())
    # print(rdd2.count())
    # cntByVal = rdd1.countByValue()
    # print(type(cntByVal))
    # print(cntByVal)
    print(rdd1.take(4))
    print(rdd1.first())
    print(rdd1.top(3))
    print(rdd1.takeOrdered(4))
    print(rdd1.takeOrdered(4, lambda x:-x))