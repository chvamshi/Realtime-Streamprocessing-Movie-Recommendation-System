#!/usr/bin/env python
# coding: utf-8

# In[1]:



import time



# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[3]:


KAFKA_TOPIC_NAME_CONS = "numtest"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'


# In[4]:


if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started ...")
    spark = SparkSession.builder.appName("MovieRecommendationSystem").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(" kafka Started ...")
    # Construct a streaming DataFrame that reads from testtopic
    transaction_detail_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS_CONS).option("subscribe", KAFKA_TOPIC_NAME_CONS).option("startingOffsets", "latest").load()
    
    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
    # Write final result into console for debugging purpose
    trans_detail_write_stream = transaction_detail_df.writeStream.trigger(processingTime='2 seconds').outputMode("update").option("truncate", "false").format("console").start()
    trans_detail_write_stream.awaitTermination()
    spark.stop()
