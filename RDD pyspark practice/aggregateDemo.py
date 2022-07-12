
from pyspark import SparkContext, SparkConf

class SumCount:
    def __init__(self, summ :int, count:int):
        self.summ=summ
        self.count = count


if __name__ == "__main__":
    conf = SparkConf().setAppName("Aggregate Demo").setMaster("local[2]")
    sc = SparkContext(conf=conf)

    sum_count = SumCount(0,0)
    seqOp = lambda sumcount,x : SumCount(sumcount.summ + x, sumcount.count+1)
    comOp = lambda sumcnt1,sumcnt2: SumCount(sumcnt1.summ+sumcnt2.summ, sumcnt1.count+sumcnt2.count)
    rdd1 = sc.parallelize([23,29,56,63,98,100,75])
    x = rdd1.aggregate(sum_count,seqOp,comOp)

    print(x.summ/x.count)