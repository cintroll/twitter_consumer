import boto3
from boto3.dynamodb.conditions import Key, Attr

tweet_table_schema = {
    'TableName':'tweets',
    'KeySchema': [
        {
            'AttributeName':'id',
            'KeyType':'HASH'
        },
        {
            'AttributeName':'category',
            'KeyType':'RANGE'
        }
    ],
    'AttributeDefinitions':[
        {
            'AttributeName': 'id',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'category',
            'AttributeType': 'S'
        },
    ],
    'ProvisionedThroughput':{
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
}

class dynamodb_table:
    def __init__(self, table_name):
        self._table_tweets_name = table_name
        self._table_tweets = boto3.resource('dynamodb').Table(self._table_tweets_name)
        # check if Tweets table exists
        try:
            self._table_tweets.creation_date_time
        except Exception:
            self.create_tweet_table()

    def write_tweets(self, tweets : list):
        self.clean_tweet_table()

        with self._table_tweets.batch_writer() as batch:
            for tweet in tweets:
                batch.put_item(Item=tweet)
    
    def create_tweet_table(self):
        tweet_table_schema['TableName'] = self._table_tweets_name
        dynamodb = boto3.resource('dynamodb')

        self._table_tweets = dynamodb.create_table(**tweet_table_schema)
        self._table_tweets.meta.client.get_waiter('table_exists').wait(TableName=self._table_tweets_name)

    def clean_tweet_table(self):
        # list all tweet
        scan = self._table_tweets.scan(FilterExpression=Attr('id').gt(0))
        # delete each tweet
        with self._table_tweets.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key={
                    'id':each['id'],
                    'category':each['category']
                })

