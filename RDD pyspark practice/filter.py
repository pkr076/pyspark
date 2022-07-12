
from pyspark import SparkContext, SparkConf

def gt2(x):
    if x>2:
        return True
    else:
        return False

if __name__== "__main__":
    conf = SparkConf().setAppName('Filter gt 2').setMaster("local[2]")
    sc = SparkContext(conf=conf)
    fun = gt2
    lst = [4,1,2,6,8,0]
    rdd = sc.parallelize(lst)
    #filtered_rdd = rdd.filter(lambda x:x>2)
    filtered_rdd = rdd.filter(fun)
    filtered_rdd.saveAsTextFile("output")

