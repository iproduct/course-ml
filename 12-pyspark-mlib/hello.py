import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *



if __name__ == '__main__':
    # Load relevant objects
    # spark = SparkSession.builder.master("local[*]").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
    spark = SparkSession.builder.master("spark://192.168.0.12:7077").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
    # sc = SparkContext('local')
    # log_txt = spark.textFile("/path/to/text/file.txt")
    # sqlContext = SparkSession.builder.getOrCreate(sc)
    #
    # # Construct fields with names from the header, for creating a DataFrame
    # header = log_txt.first()
    # fields = [StructField(field_name, StringType(), True)
    #           for field_name in header.split(',')]
    # spark.sparkContext.setLogLevel("INFO")
    textFile = spark.read.text("README.md")
    print(textFile.count())
    print(textFile.first())
    textFile.printSchema()
    linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
    print(f'Lines with Spark: {linesWithSpark.count()}')

