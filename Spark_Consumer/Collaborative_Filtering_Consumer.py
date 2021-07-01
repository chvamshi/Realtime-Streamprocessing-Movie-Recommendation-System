from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.mllib.stat import Statistics
import pandas 
import itertools
import threading
import time
import sys

done = False
#here is the animation
def animate():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if done:
            break
        sys.stdout.write('\rloading Correlation Matrix ' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rDone!     ')

t = threading.Thread(target=animate)
t.start()

#long process here
time.sleep(10)

#---------------------------------Reading Correlation Matrix--------------------------------------
correlationMatrix = pandas.read_csv('corr_matrix.csv')
done = True
correlationMatrix = correlationMatrix.set_index('title')


#---------------------------------Initialization of Topic Name--------------------------------------
KAFKA_TOPIC_NAME_CONS = "user_based_collaborative_filtering"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

#---------------------------------Movie Predictions--------------------------------------
def predict_movie(df,epoch_id):
    print("Showing Recommendations for Batch Number",epoch_id)
    test_matrix = df.toPandas()
    test_matrix = test_matrix.drop(['genres'],axis=1)
    recommend(test_matrix,correlationMatrix)
    
def get_similar(movie_name,rating,corrMatrix):
    similar_ratings = corrMatrix.get(movie_name)*(rating-2.5)
    similar_ratings = similar_ratings.sort_values(ascending=False)
    #print(type(similar_ratings))
    return similar_ratings

def recommend(test_matrix,corrMatrix):
    z1 = test_matrix.groupby('userId').groups
    listofitems = {}
    for k,v in z1.items():
        listofitems[k] = list(zip(test_matrix.iloc[v.tolist()].title.tolist(),test_matrix.iloc[v.tolist()].rating.tolist()))
    for k1,v1 in listofitems.items():
        print('\nRecommendation for user no.',k1)
        dropper = [i[0] for i in v1]
        similar_movies = pandas.DataFrame()
        for movie,rating in v1:
                similar_movies = similar_movies.append(get_similar(movie,rating,corrMatrix),ignore_index = True)


                recommended = similar_movies.sum().sort_values(ascending=False)
                recommended = recommended.drop(labels = dropper)
                print(recommended.head(5))


    
        

#---------------------------Main Starts here and Streaming data is consumed-------------------------

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started ...")
    
    spark = SparkSession.builder.appName("MovieRecommendationSystem_UserBased").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print(" kafka Started ...")
    
#--------------------Constructing a streaming DataFrame that reads from mentioned topic-------------
    transaction_detail_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS_CONS).option("subscribe", KAFKA_TOPIC_NAME_CONS).option("startingOffsets", "latest").load().selectExpr("CAST(value AS STRING)","CAST(timestamp AS STRING)")
    

#---------------------------------Printing the streaming Data----------------------------------------
    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
    
    number_schema = StructType([StructField("movieId", IntegerType(), True),StructField("title", StringType(), True),StructField("genres", StringType(), True),StructField("userId", IntegerType(), True),StructField("rating", DoubleType(), True)])
    
#-------------------- Write final result into console for debugging purpose---------------------------
    console_stream = transaction_detail_df.select(from_json(col("value"), number_schema).alias("jsonData")).select("jsonData.*")
    
    console_stream.writeStream.trigger(processingTime='30 seconds').foreachBatch(predict_movie).start()
    
    my_df = console_stream.writeStream.trigger(processingTime='30 seconds').outputMode("update").option("truncate", "false").format("console").start()
                                                             
    
    my_df.awaitTermination()
    spark.stop()