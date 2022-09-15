''' You can manually create a PySpark DataFrame using toDF() and createDataFrame() methods, both these function
 takes different signatures in order to create DataFrame from existing RDD, list, and DataFrame. You can also create
  PySpark DataFrame from data sources like TXT, CSV, JSON, ORV, Avro, Parquet, XML formats by reading from HDFS,
  S3, DBFS, Azure Blob file systems etc.'''

from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.appName('dataFrame creation demo').master('local').getOrCreate()

    columns = ["language", "users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    #Creating df from RDD
    rdd1 = spark.sparkContext.parallelize(data)
    #df1 = rdd1.toDF()
    df1 = rdd1.toDF(columns)
    #df1.printSchema()

    df2 = spark.createDataFrame(data).toDF(*columns)  '''createDataFrame takes RDD or iterable'''
    #df2.printSchema()

    '''Using createDataFrame() with the Row type'''
    rowData = map(lambda x: Row(*x), data)
    df3 = spark.createDataFrame(rowData, columns)
    #df3.printSchema()

    data2 = [("James", "", "Smith", "36636", "M", 3000),
             ("Michael", "Rose", "", "40288", "M", 4000),
             ("Robert", "", "Williams", "42114", "M", 4000),
             ("Maria", "Anne", "Jones", "39192", "F", 4000),
             ("Jen", "Mary", "Brown", "", "F", -1)
             ]

    schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("middlename", StringType(), True), \
        StructField("lastname", StringType(), True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
        ])

    df = spark.createDataFrame(data=data2, schema=schema)
    df.printSchema()
    df.show(truncate=False)