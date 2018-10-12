import twitter

class twitter_interface:
    def __init__(self, consumer_key=None, consumer_secret=None, access_token_key=None, access_token_secret=None):
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._access_token_key = access_token_key
        self._access_token_secret = access_token_secret
        self._twitter_api = twitter.Api(consumer_key=self._consumer_key
        , consumer_secret=self._consumer_secret
        , access_token_key=self._access_token_key
        , access_token_secret=self._access_token_secret)

    def search_hashtag(self, hashtag):
        return self._twitter_api.GetSearch(raw_query='q="%%23%s"&include_entities=true&count=100' % hashtag, return_json=True)