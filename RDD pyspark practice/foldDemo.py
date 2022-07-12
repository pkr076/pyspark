
from pyspark import SparkContext, SparkConf

def maximumVal(x,y):
    if(x>y):
        return x
    else:
        return y

if __name__ == "__main__":
    conf = SparkConf().setAppName("fold Demo").setMaster("local[2]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([23,29,56,63,98,100,75])
    x = rdd1.fold(0,maximumVal)
    #x = rdd1.fold(0,lambda x,y: x if x>y else y)
    print(x)