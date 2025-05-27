from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import MulticlassMetrics

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Word Count").config("spark.some.config.option",
                                                                                 "some-value").getOrCreate()

    # Load training data
    dataset = spark \
        .read \
        .format("libsvm") \
        .load("data/mllib/sample_multiclass_classification_data.txt")

    # Load training data in LIBSVM format
    # dataset = MLUtils.loadLibSVMFile(spark.sparkContext, "data/mllib/sample_multiclass_classification_data.txt")

    training, testing = tuple(dataset.randomSplit([0.7, 0.3]))
    training.cache()
    print(f"Training: {training.count()}")
    training.show()
    print(f"Testing: {testing.count()}")
    testing.show()

    lr = LogisticRegression(maxIter=30, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    model = lr.fit(training)

    # Print the coefficients and intercept for multinomial logistic regression
    print("Coefficients: \n" + str(model.coefficientMatrix))
    print("Intercept: " + str(model.interceptVector))

    trainingSummary = model.summary

    # Obtain the objective per iteration
    objectiveHistory = trainingSummary.objectiveHistory
    print("objectiveHistory:")
    for objective in objectiveHistory:
        print(objective)

    # for multiclass, we can inspect metrics on a per-label basis
    print("False positive rate by label:")
    for i, rate in enumerate(trainingSummary.falsePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("True positive rate by label:")
    for i, rate in enumerate(trainingSummary.truePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("Precision by label:")
    for i, prec in enumerate(trainingSummary.precisionByLabel):
        print("label %d: %s" % (i, prec))

    print("Recall by label:")
    for i, rec in enumerate(trainingSummary.recallByLabel):
        print("label %d: %s" % (i, rec))

    print("F-measure by label:")
    for i, f in enumerate(trainingSummary.fMeasureByLabel()):
        print("label %d: %s" % (i, f))

    accuracy = trainingSummary.accuracy
    falsePositiveRate = trainingSummary.weightedFalsePositiveRate
    truePositiveRate = trainingSummary.weightedTruePositiveRate
    fMeasure = trainingSummary.weightedFMeasure()
    precision = trainingSummary.weightedPrecision
    recall = trainingSummary.weightedRecall
    print("Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
          % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))

    #evaluation statisitics on testing set
    print("\n\n---------------------------------------------------------------")
    # Run training algorithm to build the model
    #model = LogisticRegressionWithLBFGS.train(training, numClasses=3)

    evaluationSummary = model.evaluate(testing)
    # Obtain the objective per iteration

    # for multiclass, we can inspect metrics on a per-label basis
    print("False positive rate by label:")
    for i, rate in enumerate(evaluationSummary.falsePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("True positive rate by label:")
    for i, rate in enumerate(evaluationSummary.truePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("Precision by label:")
    for i, prec in enumerate(evaluationSummary.precisionByLabel):
        print("label %d: %s" % (i, prec))

    print("Recall by label:")
    for i, rec in enumerate(evaluationSummary.recallByLabel):
        print("label %d: %s" % (i, rec))

    print("F-measure by label:")
    for i, f in enumerate(evaluationSummary.fMeasureByLabel()):
        print("label %d: %s" % (i, f))

    accuracy = evaluationSummary.accuracy
    falsePositiveRate = evaluationSummary.weightedFalsePositiveRate
    truePositiveRate = evaluationSummary.weightedTruePositiveRate
    fMeasure = evaluationSummary.weightedFMeasure()
    precision = evaluationSummary.weightedPrecision
    recall = evaluationSummary.weightedRecall
    print("Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
          % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))

    # # Compute raw scores on the test set
    # testing_rdd = testing.rdd
    # predictionAndLabels = testing_rdd.map(lambda lp: (float(model.predict(lp.features)), lp.label))
    # labels = testing_rdd.map(lambda d: d.label)
    # print(labels.collect())
    # features = testing_rdd.map(lambda d: d.features)
    # print(features.collect())
    # # Predict testing set values
    # result = model.predict(features.collect()[0])
    # predictions = features.map(lambda f: float(model.predict(f)))
    # predictions.toDF().show()
    # print(predictions.count())
    # predictionsAndLabels = predictions.zip(labels)
    # # Instantiate metrics object
    # metrics = MulticlassMetrics(predictionsAndLabels)
    # # Overall statistics
    # precision = metrics.precision(1.0)
    # recall = metrics.recall(1.0)
    # f1Score = metrics.fMeasure(1.0)
    # print("Summary Stats")
    # print("Precision = %s" % precision)
    # print("Recall = %s" % recall)
    # print("F1 Score = %s" % f1Score)
    #
    # # Statistics by class
    # labels = dataset.map(lambda lp: lp.label).distinct().collect()
    # for label in sorted(labels):
    #     print("Class %s precision = %s" % (label, metrics.precision(label)))
    #     print("Class %s recall = %s" % (label, metrics.recall(label)))
    #     print("Class %s F1 Measure = %s" % (label, metrics.fMeasure(label, beta=1.0)))
    #
    # # Weighted stats
    # print("Weighted recall = %s" % metrics.weightedRecall)
    # print("Weighted precision = %s" % metrics.weightedPrecision)
    # print("Weighted F(1) Score = %s" % metrics.weightedFMeasure())
    # print("Weighted F(0.5) Score = %s" % metrics.weightedFMeasure(beta=0.5))
    # print("Weighted false positive rate = %s" % metrics.weightedFalsePositiveRate)