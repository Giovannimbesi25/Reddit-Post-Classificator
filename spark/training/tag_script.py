import numpy as np
import pandas as pd
import os
import shutil
import pyspark.ml.feature
import pyspark.sql.functions as funcs
from pyspark.ml.feature import Tokenizer,StopWordsRemover,CountVectorizer,IDF
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import IndexToString

from pyspark.ml.classification import LogisticRegression
from pyspark import SparkContext 
from pyspark import SparkFiles
from pyspark.sql.functions import col
from pyspark.sql import SparkSession 
from pyspark.ml import Pipeline 
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import StringType





APP_NAME = 'reddit_post_classification_model_training'
APP_DATASET_PATH = './data/dataframe.csv'
APP_DATASET_FILE =  'dataframe.csv'



def main():
    
    df1 = pd.read_csv('data/subreddit_info.csv', delimiter=',',usecols=['subreddit','category_1', 'category_2']).set_index("subreddit")
    df1.dataframeName = 'subreddit_info.csv'
    df2 = pd.read_csv('data/rspct.tsv', delimiter='\t').set_index("subreddit")
    df2 = df2.join(df1).drop(["id"],axis=1)


    categories_of_interest=['electronics', 'music', 'sports', 'sex/relationships' , 'video_game', 'politics/viewpoint']
    

    subset_df=df2.loc[df2.category_1.isin(categories_of_interest), ['category_1','title']]
    subset_df.columns = ['category_1','title',]
    #subset_df.head()


    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    #Create spark dataframe from pandas
    data=spark.createDataFrame(subset_df) 
    #Clean it
    data = data.dropna(subset=('category_1'))

    tokenizer = Tokenizer(inputCol='title',outputCol='mytokens')
    stopwords_remover = StopWordsRemover(inputCol='mytokens',outputCol='filtered_tokens')
    vectorizer = CountVectorizer(inputCol='filtered_tokens',outputCol='rawFeatures')
    idf = IDF(inputCol='rawFeatures',outputCol='vectorizedFeatures')
    labelEncoder = StringIndexer(inputCol='category_1',outputCol='label').fit(data)
    
    label_dict = {'electronics':1.0, 'music':2.0, 'sports':3.0, 'sex/relationships':4.0 , 'video_game':0.0, 'politics/viewpoint':5.0}

    data = labelEncoder.transform(data)

    data.groupBy("label", "category_1") \
        .count() \
        .orderBy(col("count").desc()) \
        .show()

    (trainDF,testDF) = data.randomSplit((0.7,0.3),seed=42)

    lr = LogisticRegression(featuresCol='vectorizedFeatures',labelCol='label')

    pipeline = Pipeline(stages=[tokenizer,stopwords_remover,vectorizer,idf,lr])

    lr_model = pipeline.fit(trainDF)

    predictions = lr_model.transform(testDF)


    predictions.select('probability','category_1','label','prediction').show(10)

    evaluator = MulticlassClassificationEvaluator(labelCol='label',predictionCol='prediction',metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)

    print("Accuracy: ",accuracy)
    
    ex1 = spark.createDataFrame([
    ("What is best the best NBA player?",StringType())
    ],
    ["title"])


    ex1.show(truncate=False)
    
    pred_ex1 = lr_model.transform(ex1)
    

    lr_model.save('model')

    pred_ex1.show()


if __name__ == '__main__': main()
