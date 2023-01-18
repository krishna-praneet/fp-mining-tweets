from kafka import KafkaProducer as producer
import tweepy
import re 


# Twitter credentials
import twitter_conf as conf

# Tweet Locations
import tweet_locations as loc


ny_topic = "ny-test"
ca_topic = "ca-test"

ny_broker = "10.128.0.7:9092"
ca_broker = "10.128.0.6:9092"

ny_producer = producer(bootstrap_servers=ny_broker)
ca_producer = producer(bootstrap_servers=ca_broker)

class MyStreamer1(tweepy.StreamListener):
    
    def on_error(self, error):
        return True

    def on_status(self, status):
        if status.truncated:
            try: 
                tweet = status.extended_tweet["full_text"]
            except AttributeError:
                tweet = status.retweeted_status.extended_tweet["full_text"]
        else:
            tweet = status.text


        #tweet = re.sub(r'RT\s@\w*:\s', '', tweet)
        tweet = re.sub(r'https?.*', '', tweet)
        tweet = re.sub('\n', ' ', tweet)

        global ny_producer
        ny_producer.send(ny_topic, bytes("NY|||" + tweet, encoding='utf-8'))
        #print(status.text + "...from New York")
        return True

class MyStreamer2(tweepy.StreamListener):

    def on_status(self, status):
        if status.truncated:
            try: 
                tweet = status.extended_tweet["full_text"]
            except AttributeError:
                tweet = status.retweeted_status.extended_tweet["full_text"]
        else:
            tweet = status.text

        tweet = re.sub(r'https?.*', '', tweet)
        tweet = re.sub('\n', ' ', tweet)
        

        global ca_producer
        ca_producer.send(ca_topic, bytes("CA|||"+ tweet, encoding='utf-8'))
        #print(status.text + "...from California")
        return True

    def on_error(self, error):
        return True

cons_key = conf.CONSUMER_KEY
cons_secret_key = conf.CONSUMER_SECRET_KEY
acc_token = conf.ACCESS_TOKEN
acc_secret_token = conf.ACCESS_SECRET_TOKEN

auth = tweepy.OAuthHandler(cons_key, cons_secret_key)
auth.set_access_token(acc_token, acc_secret_token)

api = tweepy.API(auth)

stream1 = tweepy.Stream(auth=api.auth, listener=MyStreamer1())
stream2 = tweepy.Stream(auth=api.auth, listener=MyStreamer2())

stream1.filter(locations=loc.NY_LOC,is_async=True)
stream2.filter(locations=loc.CA_LOC,is_async=True)

print("Streaming tweeets ...")
