import boto3
from boto3.dynamodb.conditions import Key, Attr

tweets_table_schema = {
    'TableName':'tweets',
    'KeySchema': [
        {
            'AttributeName':'id',
            'KeyType':'HASH'
        },
        {
            'AttributeName':'hashtag',
            'KeyType':'RANGE'
        }
    ],
    'AttributeDefinitions':[
        {
            'AttributeName': 'id',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'hashtag',
            'AttributeType': 'S'
        },
    ],
    'ProvisionedThroughput':{
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
}

users_table_schema = {
    'TableName':'users',
    'KeySchema': [
        {
            'AttributeName':'id',
            'KeyType':'HASH'
        }
    ],
    'AttributeDefinitions':[
        {
            'AttributeName': 'id',
            'AttributeType': 'N'
        }
    ],
    'ProvisionedThroughput':{
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
}

statistics_table_schema = {
    'TableName':'statistics',
    'KeySchema': [
        {
            'AttributeName':'type',
            'KeyType':'HASH'
        }
    ],
    'AttributeDefinitions':[
        {
            'AttributeName': 'type',
            'AttributeType': 'N'
        }
    ],
    'ProvisionedThroughput':{
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
}

class statistics_type:
    FIVE_MOST =  1
    HOUR_DAY_GROUP = 2
    HASHTAG_LANG_GROUP = 3

class dynamodb_table:
    def __init__(self, table_name):
        self._table_tweets_name = 'tweets'
        self._table_tweets = boto3.resource('dynamodb').Table(self._table_tweets_name)
        # check if tweets table exists
        try:
            self._table_tweets.creation_date_time
        except Exception:
            self.create_tweets_table()

        self._table_users_name = 'users'
        self._table_users = boto3.resource('dynamodb').Table(self._table_users_name)
        # check if users table exists
        try:
            self._table_users.creation_date_time
        except Exception:
            self.create_users_table()

        self._table_statistics_name = 'statistics'
        self._table_statistics = boto3.resource('dynamodb').Table(self._table_statistics_name)
        # check if statistics table exists
        try:
            self._table_statistics.creation_date_time
        except Exception:
            self.create_statistics_table()
        self.clean_statistics_table()

    def write_tweets(self, tweets : list):
        self.clean_tweets_table()

        with self._table_tweets.batch_writer() as batch:
            for tweet in tweets:
                batch.put_item(Item=tweet)

    def write_users(self, users : dict):
        self.clean_users_table()

        with self._table_users.batch_writer() as batch:
            for _,user in users.items():
                batch.put_item(Item=user)

    def write_statistic(self, type, statistic) :
        statistic = {
            'type' : type,
            'data' : statistic
        }
        # write statistic
        self._table_statistics.put_item(Item=statistic)
    
    def create_tweets_table(self):
        dynamodb = boto3.resource('dynamodb')

        self._table_tweets = dynamodb.create_table(**tweets_table_schema)
        self._table_tweets.meta.client.get_waiter('table_exists').wait(TableName=self._table_tweets_name)

    def create_users_table(self):
        dynamodb = boto3.resource('dynamodb')

        self._table_users = dynamodb.create_table(**users_table_schema)
        self._table_users.meta.client.get_waiter('table_exists').wait(TableName=self._table_users_name)

    def create_statistics_table(self):
        dynamodb = boto3.resource('dynamodb')

        self._table_statistics = dynamodb.create_table(**statistics_table_schema)
        self._table_statistics.meta.client.get_waiter('table_exists').wait(TableName=self._table_statistics_name)

    def clean_tweets_table(self):
        # list all tweet
        scan = self._table_tweets.scan(FilterExpression=Attr('id').gt(0))
        # delete each tweet
        with self._table_tweets.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key={
                    'id':each['id'],
                    'hashtag':each['hashtag']
                })

    def clean_users_table(self):
        # list all users
        scan = self._table_users.scan(FilterExpression=Attr('id').gt(0))
        # delete each users
        with self._table_users.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key={
                    'id':each['id']
                })

    def clean_statistics_table(self):
        # list all users
        scan = self._table_statistics.scan(FilterExpression=Attr('type').gt(0))
        # delete each users
        with self._table_statistics.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key={
                    'type':each['type']
                })
