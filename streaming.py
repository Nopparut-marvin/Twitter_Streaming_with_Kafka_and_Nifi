from kafka import KafkaProducer
from tweepy import Stream
import os

"""API ACCESS KEY"""

access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
api_key = os.getenv('API_KEY')
api_key_secret = os.getenv('API_KEY_SECRET')

"""KAFKA SETTING"""

topic_name = "test-topic"
bootstrap_servers = "localhost:9092"

""""FILTER PARAMETER"""

keywords_list = ["hello kafka"]

# Kafka producer initialization
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Twitter streaming initialization
class TwitterStreamer():
    def __init__(self) :
        pass
    def stream_tweets(self, keywords):
        stream = TwitterListener(api_key,api_key_secret,access_token,access_token_secret)
        stream.filter(track=keywords)

class TwitterListener(Stream):
    def on_data(self,data):
        producer.send(topic_name,data)

if __name__ == "__main__":
    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(keywords_list)


