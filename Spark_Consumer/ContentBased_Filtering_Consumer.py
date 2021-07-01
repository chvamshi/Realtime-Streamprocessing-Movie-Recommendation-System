from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.mllib.stat import Statistics
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
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
        sys.stdout.write('\rloading Similiarity matrix ' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rDone!     ')

t = threading.Thread(target=animate)
t.start()

#long process here
time.sleep(10)

#---------------------------------Reading data and Creating Count Vector--------------------------------------
df = pd.read_csv('movies.csv')

genre_labels = set()
for s in df['genres'].str.split('|').values:
    genre_labels = genre_labels.union(set(s))
 

def count_word(dataset, ref_col, census):
    keyword_count = dict()
    for s in census: 
        keyword_count[s] = 0
    for census_keywords in dataset[ref_col].str.split('|'):        
        if type(census_keywords) == float and pd.isnull(census_keywords): 
            continue        
        for s in [s for s in census_keywords if s in census]: 
            if pd.notnull(s): 
                keyword_count[s] += 1
    #______________________________________________________________________
    # convert the dictionary in a list to sort the keywords by frequency
    keyword_occurences = []
    for k,v in keyword_count.items():
        keyword_occurences.append([k,v])
    keyword_occurences.sort(key = lambda x:x[1], reverse = True)
    return keyword_occurences, keyword_count

keyword_occurences, dum = count_word(df, 'genres', genre_labels)

# Break up the big genre string into a string array
df['genres'] = df['genres'].str.split('|')
# Convert genres to string value
df['genres'] = df['genres'].fillna("").astype('str')

tf = TfidfVectorizer(analyzer='word',ngram_range=(1, 2),min_df=0, stop_words='english')
tfidf_matrix = tf.fit_transform(df['genres'])

cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

#-----------Mapping of Movie title and INdex-----
titles = df['title']
indices = pd.Series(df.index, index=df['title'])

done = True


#---------------------------------Initialization of Topic Name--------------------------------------

KAFKA_TOPIC_NAME_CONS = "content_based_filtering"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic2"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'


#---------------------------------Movie Predictions for Each Batches--------------------------------------
def predict_movie(df,epoch_id):
    print("Showing Recommendations for Batch Number",epoch_id)
    test_matrix = df.toPandas()
    
    for batch in zip(test_matrix['userId'],test_matrix['title'],test_matrix['rating']):
        if(batch[2] < 3):
            print("Since user ",batch[0]," watched ",batch[1]," and Did not like it we are not going to recommend any movie in that genre")
        else:
            print("Since user ",batch[0]," watched ",batch[1]," we reommend him\n")
            print(genre_recommendations(batch[1]).head(5))
        print()   
    
def genre_recommendations(title):
    idx = indices[title]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:21]
    movie_indices = [i[0] for i in sim_scores]
    return titles.iloc[movie_indices]


#---------------------------Main Starts here and Streaming data is consumed-------------------------

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started ...")
    
    spark = SparkSession.builder.appName("MovieRecommendationSystem_ContentBased").getOrCreate()
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
    
    my_df = console_stream.writeStream.trigger(processingTime='30 seconds').foreachBatch(predict_movie).start()
    
#     my_df = console_stream.writeStream.trigger(processingTime='30 seconds').outputMode("update").option("truncate", "false").format("console").start()
                                                             
    
    my_df.awaitTermination()
    spark.stop()