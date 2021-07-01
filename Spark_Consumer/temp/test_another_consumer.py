from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.mllib.stat import Statistics
import pandas




#---------------------------------Initialization of Topic Name--------------------------------------
KAFKA_TOPIC_NAME_CONS = "user collaborative filtering"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
#
def predict_movie(df,epoch_id):
    print("show")
    test_matrix = df.toPandas()
    ratings =  test_matrix.pivot_table(index=['userId'],columns=['title'],values='rating')
    userRatings = ratings.fillna(0,axis=1)
    corrMatrix = userRatings.corr(method='pearson')
    recommend(test_matrix,corrMatrix)
    
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
        print('Recommendation for user no.',k1)
        similar_movies = pandas.DataFrame()
        for movie,rating in v1:
                similar_movies = similar_movies.append(get_similar(movie,rating,corrMatrix),ignore_index = True)


        print(similar_movies.sum().sort_values(ascending=False).head(10))


    
        

#---------------------------Main Starts here and Streaming data is consumed-------------------------

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started ...")
    
    spark = SparkSession.builder.appName("MovieRecommendationSystem").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print(" kafka Started ...")
    
#--------------------Constructing a streaming DataFrame that reads from mentioned topic-------------
    transaction_detail_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS_CONS).option("subscribe", KAFKA_TOPIC_NAME_CONS).option("startingOffsets", "latest").load().selectExpr("CAST(value AS STRING)","CAST(timestamp AS STRING)")
    

#---------------------------------Printing the streaming Data----------------------------------------
    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
    
    number_schema = StructType([StructField("userId", IntegerType(), True),StructField("movieId", IntegerType(), True),StructField("rating", DoubleType(), True),StructField("title", StringType(), True)])
    
#-------------------- Write final result into console for debugging purpose---------------------------
    console_stream = transaction_detail_df.select(from_json(col("value"), number_schema).alias("jsonData")).select("jsonData.*")
    
    console_stream.writeStream.trigger(processingTime='60 seconds').foreachBatch(predict_movie).start()
    
    my_df = console_stream.writeStream.trigger(processingTime='60 seconds').outputMode("update").option("truncate", "false").format("console").start()
                                                             
    
    my_df.awaitTermination()
    spark.stop()
