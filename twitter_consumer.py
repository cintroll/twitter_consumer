from twitter_interface import twitter_interface
from dynamodb import dynamodb_table, statistics_type
from datetime import datetime
import json
import time


def twitter_datetime(datetime):
    return int(time.mktime(time.strptime(datetime,'%a %b %d %H:%M:%S +0000 %Y')) // 3600 * 3600)

def query_tags(twitter_api : twitter_interface):
    tweets = []
    users = {}

    with open("tags.json", 'r') as twt_tags:
        tags = json.load(twt_tags)['tags']
        
        for tag in tags:                
            result = twitter_api.search_hashtag(tag)
            for tweet in result['statuses']:
                tweet_entry = {}
                tweet_entry['id'] = tweet['id']
                tweet_entry['hashtag'] = tag
                tweet_entry['text'] = tweet['text']  
                tweet_entry['date_time'] = tweet['created_at']
                # user info
                user_id = tweet['user']['id']
                users[user_id] = {}
                users[user_id]['id'] = tweet['user']['id']
                users[user_id]['name'] = tweet['user']['screen_name']
                users[user_id]['followers'] = tweet['user']['followers_count']
                users[user_id]['lang'] = tweet['user']['lang']
                tweet_entry['user_id'] = user_id
                # save tweet
                tweets.append(tweet_entry)
    
    return tweets, users

# find the five users with most followers
def five_most_followed(users : dict):
    statistic = []

    sorted_users = sorted(users.values(), key=lambda item : item['followers'], reverse=True)
    count = 0

    for user in sorted_users:
        if count >= 5:
            break
        else:
            count += 1
        statistic.append({
            'user_id': user['id'],
            'user_followers':user['followers']
        })

    return statistic

# group and count tweets by hour of day
def tweets_hour_of_day(tweets : list):
    statistic = []

    date_time_group = {}
    for tweet in tweets:
        timestamp = twitter_datetime(tweet['date_time'])
        date_time_group.setdefault(timestamp, 0) 
        date_time_group[timestamp] += 1

    for key, value in date_time_group.items():
        statistic.append({
            'timestamp' : key,
            'count' : value
        })

    return statistic

# group and count tweets by lang and hashtag
def tweets_lang_tag(tweets : list, users : dict):
    statistic = []

    hashtag_group = {}
    for tweet in tweets:
        hashtag = tweet['hashtag']
        lang = users[tweet['user_id']]['lang']
        hashtag_group.setdefault(hashtag, {})
        hashtag_group[hashtag].setdefault(lang, 0)
        hashtag_group[hashtag][lang] += 1

    for hashtag, lang_group in hashtag_group.items():
        lang_count = []
        for lang, count in lang_group.items():
            lang_count.append({
                'lang': lang,
                'count': count
            })
        statistic.append({
            'hashtag': hashtag,
            'lang_group': lang_count
        })

    return statistic

if __name__ == "__main__":
    data_base = dynamodb_table('tweets')
    with open("twitter_keys.json", 'r') as twt_keys:
        keys = json.load(twt_keys)\
        # load twitter api
        twitter_api = twitter_interface(**keys)
        # query tweet from twitter api
        tweets, users = query_tags(twitter_api)       
        # save tweets on data base
        data_base.write_tweets(tweets)
        # save users on data base
        data_base.write_users(users)
        # generate statistics and save
        data_base.write_statistic(statistics_type.FIVE_MOST, five_most_followed(users))
        data_base.write_statistic(statistics_type.HOUR_DAY_GROUP, tweets_hour_of_day(tweets))
        data_base.write_statistic(statistics_type.HASHTAG_LANG_GROUP, tweets_lang_tag(tweets, users))

