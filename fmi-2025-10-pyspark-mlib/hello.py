from pyspark.sql import SparkSession


if __name__ == '__main__':
    # Load relevant objects
    # spark = SparkSession.builder.master("spark://192.168.0.12:7077").appName("Word Count")\

    # spark = SparkSession.builder \
    #     .master("spark://192.168.0.17:7077") \
    #     .config("spark.some.config.option", "some-value")\
    #     .getOrCreate()

    spark = SparkSession.builder\
        .master("local[*]")\
        .config("spark.some.config.option", "some-value")\
        .getOrCreate()

    # spark.sparkContext.setLogLevel("INFO")
    spark.sparkContext.setLogLevel("ERROR")
    textFile = spark.read.text("README.md")
    print(textFile.count())
    print(textFile.first())
    textFile.printSchema()
    textFile.show(10, False)

    linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
    print(f'Lines with Spark: {linesWithSpark.count()}')

    spark.stop()

