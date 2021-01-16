import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.ml.classification import LogisticRegression


# def parsePoint(line):
#     """
#     Parse a line of text into an MLlib LabeledPoint object.
#     """
#     values = [float(s) for s in line.split(' ')]
#     if values[0] == -1:   # Convert -1 labels to 0 for MLlib
#         values[0] = 0
#     return LabeledPoint(values[0], values[1:])


if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Usage: logistic_regression <file> <iterations>", file=sys.stderr)
    #     sys.exit(-1)
    # sc = SparkContext(appName="PythonLR")
    # points = sc.textFile(sys.argv[1]).map(parsePoint)
    # iterations = int(sys.argv[2])
    # model = LogisticRegressionWithSGD.train(points, iterations)
    # print("Final weights: " + str(model.weights))
    # print("Final intercept: " + str(model.intercept))
    # sc.stop()

    spark = SparkSession.builder.master("local[*]").appName("Word Count").config("spark.some.config.option",
                                                                                 "some-value").getOrCreate()
    # Load training data
    training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    lrModel = lr.fit(training)

    # Print the coefficients and intercept for logistic regression
    print("Coefficients: " + str(lrModel.coefficients))
    print("Intercept: " + str(lrModel.intercept))

    # We can also use the multinomial family for binary classification
    mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

    # Fit the model
    mlrModel = mlr.fit(training)

    # Print the coefficients and intercepts for logistic regression with multinomial family
    print("Multinomial coefficients: " + str(mlrModel.coefficientMatrix))
    print("Multinomial intercepts: " + str(mlrModel.interceptVector))