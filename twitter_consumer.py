from twitter_interface import twitter_interface
import json

if __name__ == "__main__":
    with open("twitter_keys.json", 'r') as f:
        keys = json.load(f)
        interface = twitter_interface(**keys)

        result = interface.search_hashtag("openbanking")
        print(len(result['statuses']))
        for tweet in result['statuses']:
            print(tweet['text'])
            print("\n")
