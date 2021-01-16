from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors, DenseMatrix
from pyspark.mllib.linalg.distributed import RowMatrix

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Word Count").config("spark.some.config.option",
                                                                                 "some-value").getOrCreate()
    rows = spark.sparkContext.parallelize([
        Vectors.sparse(5, {1: 1.0, 3: 7.0}),
        Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
        Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    ])

    mat = RowMatrix(rows)
    # Compute the top 4 principal components.
    # Principal components are stored in a local dense matrix.
    pc = mat.computePrincipalComponents(3)

    print(pc)

    # Project the rows to the linear space spanned by the top 4 principal components.
    projected = mat.multiply(pc)
    print(projected.rows.collect())