from pyspark.sql import SparkSession
from pyspark.ml.stat import Summarizer
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Word Count").config("spark.some.config.option",
                                                                                 "some-value").getOrCreate()

    df = spark.sparkContext.parallelize([Row(weight=1.0, features=Vectors.dense(1.0, 1.0, 1.0)),
                         Row(weight=0.0, features=Vectors.dense(1.0, 2.0, 3.0))]).toDF()

    # create summarizer for multiple metrics "mean" and "count"
    summarizer = Summarizer.metrics("mean", "count")

    # compute statistics for multiple metrics with weight
    df.select(summarizer.summary(df.features, df.weight)).show(truncate=False)

    # compute statistics for multiple metrics without weight
    df.select(summarizer.summary(df.features)).show(truncate=False)

    # compute statistics for single metric "mean" with weight
    df.select(Summarizer.mean(df.features, df.weight)).show(truncate=False)

    # compute statistics for single metric "mean" without weight
    df.select(Summarizer.mean(df.features)).show(truncate=False)