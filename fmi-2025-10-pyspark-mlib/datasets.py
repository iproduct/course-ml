from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Word Count").config("spark.some.config.option",
                                                                                 "some-value").getOrCreate()
    # load images
    df = spark.read.format("image").option("dropInvalid", True).load("data/mllib/images/origin/kittens")
    df.select("image.origin", "image.width", "image.height").show(truncate=False)
    df.show()

    # load features
    df = spark.read.format("libsvm").option("numFeatures", "780").load("data/mllib/sample_libsvm_data.txt")
    df.show(20, False)