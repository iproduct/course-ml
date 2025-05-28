import re

from pyspark import SparkConf
from pyspark.ml.feature import Word2Vec
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import regexp, udf
from pyspark.sql.types import ArrayType, StringType

# from nltk.tokenize import sent_tokenize, word_tokenize

if __name__ == "__main__":
    # with open("data/allice.txt", encoding='utf-8') as f:
    #     with open("data/allice_tokenized.txt", 'w', encoding='utf-8') as out:
    #         for line in f:
    #             out.write(' '.join(re.split(r'\W', line)) + '\n')


    # findspark.init(r'D:\CourseDML\spark-3.5.5-bin-hadoop3')

    # conf = SparkConf().set('spark.executor.memory','4g').set('spark.driver.memory','4g')

    spark = (SparkSession.builder
             .master("spark://10.108.5.92:7077")
             .appName("Word2Vec")
             .config('spark.executor.memory','8g')
             .config('spark.driver.memory','8g')
             .getOrCreate())

    spark.sparkContext.setLogLevel("INFO")

    # split_by_nonword= spark.udf.register('split_by_nonword', lambda s: re.split(r'\W', s), ArrayType(StringType()))

    # inp = spark.sparkContext.textFile("data/text8.txt").map(lambda row: row.split(" "))

    def df_split(df: DataFrame) -> DataFrame:
        result = []
        for row in df.toLocalIterator():
            result.append((row.value.split(' '),))
        return spark.createDataFrame(result, ["sentence"])

    # inp = spark.read.text("data/allice_tokenized.txt").transform(df_split)
    inp = spark.read.text("data/text8.txt").transform(df_split)

    inp.printSchema()
    inp.show(n = 10, truncate=150)

    word2vec = Word2Vec(vectorSize=10, seed=42, inputCol="sentence", outputCol="model")
    model = word2vec.fit(inp)

    model.getVectors().show(1000, 200)

    model.write().overwrite().save('models/word2vec_text8')

    # synonyms = model.findSynonyms('economic', 5)
    synonyms = model.findSynonymsArray('game', 10)

    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))