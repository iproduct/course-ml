import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *



if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
    # spark.sparkContext.setLogLevel("INFO")
    df = spark.read.option("header", "true").option("delimiter", ",").csv("Person_csv.csv")
    print(df.count())
    print(df.first())
    print(df.head())
    df.printSchema()
    df.show(10)
    personsFromTexas = df.filter(df.AddressState.contains("Texas"))
    print(f'Persons from Texas: {personsFromTexas.count()}')
    personsFromTexas.show(10)

    # Registers the DataFrame in form of view
    df.createOrReplaceTempView("person")

    # Actual SparkSQL query
    sqlPersonDF = spark.sql(
        """
    SELECT
      PersonID AS PersonKey,
        'XYZ' AS IdentifierName,
        PersonIndex AS Extension,
        'A' AS Status
      FROM person
      WHERE
        PersonID IS NOT NULL AND PersonIndex IS NOT NULL
      UNION
      SELECT
        PersonID AS PersonKey,
        'ABC' AS IdentifierName,
        RecordNumber AS Extension,
        'A' AS RecordStatus
      FROM person
      WHERE
        PersonID IS NOT NULL AND RecordNumber IS NOT NULL
      UNION
      SELECT
        PersonID AS PersonKey,
        'MNO' AS IdentifierName,
        SSN AS Extension,
        'A' AS RecordStatus
      FROM person
      WHERE
        PersonID IS NOT NULL AND SSN IS NOT NULL
        """)

    # Print the results
    sqlPersonDF.show(50)