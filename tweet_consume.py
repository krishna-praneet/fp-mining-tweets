from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime
from fp_growth import *
import pytz

# Spark entry points
sc = SparkContext(appName='TwitterApp')
ssc = StreamingContext(sc, 15)
ss = SparkSession.builder.appName("TwitterApp").getOrCreate()

# Input paths 
files_dir = "/home/gudipaty_praneet/assignment5/"
ny_file = files_dir + "files/ny_tweets.txt"
ca_file = files_dir + "files/ca_tweets.txt"
ny_save_file = files_dir + "ny_tweets.json"
ca_save_file = files_dir + "ca_tweets.json"


# Kafka Conf
ny_topic = "ny-test"
ca_topic = "ca-test"
ks = KafkaUtils.createDirectStream(ssc, [ny_topic, ca_topic], {"metadata.broker.list": "10.128.0.7:9092, 10.128.0.6:9092"})


tz = pytz.timezone('Asia/Kolkata')


lines = ks.map(lambda x: x[1]) 
transform1 = lines.map(lambda tweet: tweet.strip().split('|||'))
transform2 = transform1.map(lambda tweet: [tweet[0], tweet[1],  datetime.now(tz).strftime("%d-%m-%y %H:%M:%S")])
transform2.pprint()


def processRdd(rdd):
    '''
    Function to process the rdds indiviually that arrive in the dstream
    '''
    if not rdd.isEmpty():
        
        # Create dataframe
        global ss
        df = ss.createDataFrame(rdd, schema=['from', 'tweets','time'])
        df.show()

        # Filter location based tweets
        ny_tweets = df.filter(df['from'] == 'NY').collect()
        ca_tweets = df.filter(df['from'] == 'CA').collect()

        # Save to file
        with open(ny_file, "a") as file_writer:
            for record in ny_tweets:
                file_writer.write(str(record['tweets']) + "|||" + str(record['time']) + "\n")
        
        with open(ca_file, "a") as file_writer:
            for record in ca_tweets:
                file_writer.write(str(record['tweets']) + "|||" + str(record['time']) + "\n")
        


transform2.foreachRDD(processRdd)

ssc.start()
ssc.awaitTermination()
