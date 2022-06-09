from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import types as st
from pyspark.sql.functions import from_json, col
from pyspark.sql import IndexToString

APP_NAME = 'reddit-streaming-class-prediction'
APP_BATCH_INTERVAL = 1

def get_record_schema():
    return st.StructType([
        st.StructField("title",        st.StringType()),
    ])


def main():

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    model = PipelineModel.load("model")
    schema = get_record_schema()

    df = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'broker:9092') \
        .option('subscribe', 'reddit_post') \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .selectExpr("data.*")

    results = model.transform(df)
    

    results.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == '__main__': main()