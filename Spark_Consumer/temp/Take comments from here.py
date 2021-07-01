from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def predict_movie(df,epoch_id=0):
    df.groupby('userId').pivot('title').sum('rating').show()


# Set topic name 
KAFKA_TOPIC_NAME = "userdata"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"


if __name__ == "__main__":
    
    # Create a SparkSession
    spark = SparkSession \
        .builder \
        .appName("Recommendation System for Movies") \
        .getOrCreate()
    # set logs only Error's 
    spark.sparkContext.setLogLevel("ERROR")

    
    # Subscribe to the topic
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS_CONS) \
      .option("subscribe", KAFKA_TOPIC_NAME) \
      .option("startingOffsets", "latest") \
      .load()
    # Whatever the data sent by the prodcuer is bascially goes into 'value'(in binary type) feild of default schema of Structured Streaming
    # So, Convert data from Kafka into String type
    # If not casted to String, then while parsing the data error's will come    
    # Note: If producer also sent datausing other feilds, cast them also into string ot anyother format requires    
    df = df.selectExpr("CAST(value AS STRING)")
    
    # Print Schema
    df.printSchema()
    
    # The data in 'value' will be in binary format 
    # A row of 'value' feild contains a json(most commonly) and we need to conver each json file into a row of newly created schema
    # Create a schema to read the streamed data in a proper order
    user_schema = StructType([StructField("userId", IntegerType(), True),
                                 StructField("movieId", IntegerType(), True),
                                 StructField("rating", FloatType(), True),
                                 StructField("title", StringType(), True)
                                ])
    # Parse JSON data and format it    
    # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.to_json.html
    fomatted_df = df.select(from_json(col("value"), user_schema).alias("userData")).select("userData.*")
    
    # To show the output as it is
#     query = fomatted_df.writeStream.trigger(processingTime='6 seconds').outputMode("update").option("truncate", "false").format("console").start()
    
    # Perform some operation on the formatted data
    query = fomatted_df.writeStream.trigger(processingTime='6 seconds').foreachBatch(predict_movie).start()
    
    # Sample operation on formatted_df
#     my_df = fomatted_df.select(['title','rating'])
#     query = my_df.writeStream.format('console').trigger(processingTime='6 seconds').outputMode("update").start()

    
    # Wait for the writestream operation to be over
    query.awaitTermination()
    spark.stop()
