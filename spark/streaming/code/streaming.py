from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import types as st
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import create_map, lit
from elasticsearch import Elasticsearch

APP_NAME = 'reddit-streaming-class-prediction'
APP_BATCH_INTERVAL = 1

elastic_host="https://es01:9200"
elastic_index="reddit_post"

es = Elasticsearch(
    elastic_host,
    ca_certs="/app/certs/ca/ca.crt",
    basic_auth=("elastic", "passwordTAP"), 
)

def get_record_schema():
    return st.StructType([
        st.StructField("title", st.StringType()),
    ])

label_dict = {'1.0':'electronics', '2.0':'music', '3.0':'sports','4.0': 'sex/relationships','0.0': 'video_game','5.0': 'politics/viewpoint'}


def process_batch(batch_df, batch_id):
    for idx, row in enumerate(batch_df.collect()):
        row_dict = row.asDict()
        print(row_dict)
        row_dict['categoria'] = label_dict[str(row['prediction'])]
        
        id = f'{batch_id}-{idx}'
        resp = es.index(index=elastic_index, id=id, document=row_dict)
        print(resp)


def main():

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #label_dict = {'electronics':1.0, 'music':2.0, 'sports':3.0, 'sex/relationships':4.0 , 'video_game':0.0, 'politics/viewpoint':5.0}


    model = PipelineModel.load("model")
    schema = get_record_schema()

    df = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'broker:9092') \
        .option('subscribe', 'reddit_post') \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .selectExpr("data.*")

    results = model.transform(df)
    
    results = results.select("filtered_tokens","prediction")

    results.writeStream\
        .foreachBatch(process_batch) \
        .start() \
        .awaitTermination()


if __name__ == '__main__': main()


#dev tools GET reddit/_search
#GET _cat/indices
