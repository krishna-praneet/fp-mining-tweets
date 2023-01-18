from pyspark import SparkContext
from datetime import datetime, timedelta  
from pyspark.mllib.fpm import FPGrowth
import pytz
import os 
import time

def fp_growth(sc, minSupport, numPartitions, file_path, save_path):

    # Current times
    tz = pytz.timezone('Asia/Kolkata')
    now_time = datetime.now(tz)
    delta = timedelta(minutes = 10)


    data = sc.textFile(file_path)


    tweets = data.map(lambda line: line.strip().split('|||'))
    tweets_adjusted = tweets.map(lambda row:[row[0],  datetime.strptime(row[1], "%d-%m-%y %H:%M:%S")])
    tweets_tf = tweets_adjusted.filter(lambda row: row[1] >= (now_time.replace(tzinfo=None) - delta) and row[1] <= now_time.replace(tzinfo=None))
    filtered = tweets_tf.map(lambda row: row[0])
    transacs = filtered.map(lambda row: row.strip().split(' '))
    counts = transacs.count()
    transacs_filtered = transacs.map(lambda transac: [i for n,i in enumerate(transac) if i not in transac[:n]])
    model = FPGrowth.train(transacs_filtered, minSupport, numPartitions)
    result = model.freqItemsets().collect()
    fp = ""
    for row in result:
        fp = fp + str(row) + ","
    print(counts)
    count = str(counts)
    final_result = "{\"count\":" + count + ", \"fp\": \"" + fp + "\"}"
    print(final_result)
    
    os.remove(save_path)
    with open(save_path, "w") as writer:
        writer.write(final_result)


if __name__ == "__main__":

    files_dir = "/home/gudipaty_praneet/assignment5/"
    ny_file = files_dir + "files/ny_tweets.txt"
    ca_file = files_dir + "files/ca_tweets.txt"
    ny_save_file = files_dir + "ny_tweets.json"
    ca_save_file = files_dir + "ca_tweets.json"

    sc = SparkContext(appName="FPGrowth")

    while True:
        fp_growth(sc, 0.1, 2, ny_file, ny_save_file)
        fp_growth(sc, 0.1, 2, ca_file, ca_save_file)
        time.sleep(15)


        

