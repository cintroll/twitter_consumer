import boto3

class dynamodb_table:
    def __init__(self, table_name):
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    def write_entries(self, entries : list):
        with self._table.batch_writer() as batch:
            for entry in entries:
                batch.put_item(Item=entry)