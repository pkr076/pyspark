from pyspark import SparkContext, SparkConf

def fun(line:str):
    words = line.split(' ')
    lst = []
    for word in words:
        lst.append((word,1))
    return  lst


if __name__ == "__main__":
    conf = SparkConf().setAppName("flatmapToPair Demo").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    ipRdd = sc.textFile('abc.txt')
    opRdd = ipRdd.flatMap(fun)
    opRdd.saveAsTextFile("output")