from twitter_interface import twitter_interface
from dynamodb import dynamodb_table
import json
import time

def twitter_datetime(datetime):
    return int(time.mktime(time.strptime(datetime,'%a %b %d %H:%M:%S +0000 %Y')) // 3600 * 3600)

if __name__ == "__main__":
    tweets_table = dynamodb_table('tweets')
    with open("twitter_keys.json", 'r') as twt_keys:
        keys = json.load(twt_keys)
        interface = twitter_interface(**keys)

        with open("tags.json", 'r') as twt_tags:
            tags = json.load(twt_tags)['tags']
            tweets = []
            for tag in tags:                
                result = interface.search_hashtag(tag)
                for tweet in result['statuses']:
                    tweet_entry = {}
                    tweet_entry['id'] = tweet['id']
                    tweet_entry['category'] = tag
                    tweet_entry['text'] = tweet['text']  
                    tweet_entry['date_time'] = twitter_datetime(tweet['created_at'])
                    tweet_entry['user_id'] = tweet['user']['id']
                    tweet_entry['user_name'] = tweet['user']['screen_name']
                    tweet_entry['user_name'] = tweet['user']['screen_name']
                    tweet_entry['user_followers'] = tweet['user']['followers_count']
                    tweet_entry['user_lang'] = tweet['user']['lang']
                    tweets.append(tweet_entry)
            # save tweets on data base
            tweets_table.write_tweets(tweets)

