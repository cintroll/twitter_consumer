import twitter

class twitter_interface:
    def __init__(self, **kwargs):
        self.consumer_key = kwargs['consumer_key']
        self.consumer_secret = kwargs['consumer_secret']
        self.access_token_key = kwargs['access_token_key']
        self.access_token_secret = kwargs['access_token_secret']
        self.twitter_api = twitter.Api(consumer_key=self.consumer_key
        , consumer_secret=self.consumer_secret
        , access_token_key=self.access_token_key
        , access_token_secret=self.access_token_secret)

    def search_hashtag(self, hashtag):
        return self.twitter_api.GetSearch(raw_query='q="%%23%s"&include_entities=true&count=100' % hashtag, return_json=True)