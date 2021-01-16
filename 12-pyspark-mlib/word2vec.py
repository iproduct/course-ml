from pyspark.sql import SparkSession
from pyspark.mllib.feature import Word2Vec

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Word Count").config("spark.some.config.option",
                                                                                 "some-value").getOrCreate()

    # inp = spark.sparkContext.textFile("data/text8.txt").map(lambda row: row.split(" "))
    inp = spark.sparkContext.textFile("data/mllib/sample_lda_data.txt").map(lambda row: row.split(" "))

    word2vec = Word2Vec()
    model = word2vec.fit(inp)

    # synonyms = model.findSynonyms('economic', 5)
    synonyms = model.findSynonyms('0', 5)

    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))