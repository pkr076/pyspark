
from pyspark import SparkContext,SparkConf

class SumCount():
    def __init__(self, total,count):
        self.total = total
        self.count = count


if __name__ == "__main__":
    conf = SparkConf().setAppName("combineByKey").setMaster("local")
    sc = SparkContext(conf=conf)

    lst = [("prashant",31),("kanhaiya",27),("prashant",29),("kanhaiya",31),("xyz",31),("abc",27)]
    rdd1 = sc.parallelize(lst)

    sum_count = lambda x: SumCount(x,1)
    seqOp = lambda obj, x : SumCount(x+obj.total, obj.count+1)
    comOp = lambda obj1,obj2: SumCount(obj1.total+obj2.total,obj1.count+obj2.count)

    rdd2 = rdd1.combineByKey(sum_count,seqOp,comOp)
    rdd3 = rdd2.mapValues(lambda obj: (float)(obj.total/obj.count))
    rdd3.saveAsTextFile('output')