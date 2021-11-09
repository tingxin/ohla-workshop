import boto3
from setting import *

if __name__ == '__main__':

    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    kinesis_client = session.client('kinesis', region_name=REGION)
    response = kinesis_client.describe_stream(StreamName=STREAM_NAME)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=STREAM_NAME,
                                                       ShardId=shard_id,
                                                       ShardIteratorType='LATEST')
    my_shard_iterator = shard_iterator['ShardIterator']
    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator, Limit=100)
    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=100)
        if len(record_response['Records']) > 0:
            for record in record_response['Records']:
                print(record)
