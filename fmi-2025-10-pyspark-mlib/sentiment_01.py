# Import the required modules and classes
import sparknlp
import pandas as pd
from sparknlp.base import DocumentAssembler, Pipeline, LightPipeline
from sparknlp.annotator import (
    UniversalSentenceEncoder,
    SentimentDLModel
)
import pyspark.sql.functions as F

if __name__ == '__main__':
    # Start Spark Session
    spark = sparknlp.start()

    documentAssembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")

    use = UniversalSentenceEncoder.pretrained("tfhub_use", "en") \
        .setInputCols(["document"]) \
        .setOutputCol("sentence_embeddings")
    sentimentdl = SentimentDLModel.pretrained("sentimentdl_use_twitter", "en") \
        .setInputCols(["sentence_embeddings"]) \
        .setOutputCol("sentiment")
    nlpPipeline = Pipeline(
        stages=[
            documentAssembler,
            use,
            sentimentdl
        ])
    text_list = [
        """@Mbjthegreat i really dont want AT&T phone service..they suck when it comes to having a signal""",
        """holy crap. I take a nap for 4 hours and Pitchfork blows up my twitter dashboard. I wish I was at Coachella.""",
        """@Susy412 he is working today  ive tried that still not working..... hmmmm!! im rubbish with computers haha!""",
        """Brand New Canon EOS 50D 15MP DSLR Camera Canon 17-85mm IS Lens ...: Web Technology Thread, Brand New Canon EOS 5.. http://u.mavrev.com/5a3t""",
        """Watching a programme about the life of Hitler, its only enhancing my geekiness of history.""",
        """GM says expects announcment on sale of Hummer soon - Reuters: WDSUGM says expects announcment on sale of Hummer .. http://bit.ly/4E1Fv""",
        """@accannis @edog1203 Great Stanford course. Thanks for making it available to the public! Really helpful and informative for starting off!""",
        """@the_real_usher LeBron is cool.  I like his personality...he has good character.""",
        """@sketchbug Lebron is a hometown hero to me, lol I love the Lakers but let's go Cavs, lol""",
        """@PDubyaD right!!! LOL we'll get there!! I have high expectations, Warren Buffet style.""",
    ]

    empty_df = spark.createDataFrame([['']]).toDF("text")
    model = nlpPipeline.fit(empty_df)
    df = spark.createDataFrame(pd.DataFrame({"text": text_list}))
    result = model.transform(df)

    result.select(
        F.explode(
            F.arrays_zip(
                result.document.result,
                result.sentiment.result)).alias("cols")
    ).select(
        F.expr("cols['0']").alias("document"),
        F.expr("cols['1']").alias("sentiment")
    ).show(truncate=False
           )
