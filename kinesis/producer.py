from datetime import datetime, timedelta
import json
import random
import boto3
from setting import *
import time


def get_price_data(interval):
    ts = datetime.now() + timedelta(seconds=interval)
    return {
        'event_time': ts.strftime("%Y-%m-%d %H:%M:%S"),
        'name': random.choice(['AAPL', 'AMZN', 'MSFT', 'GOOG', 'FB']),
        'price': round(random.random() * 2000 + 100, 2)}


def get_tx_data(interval):
    ts = datetime.now() + timedelta(seconds=interval)
    return {
        'event_time': ts.strftime("%Y-%m-%d %H:%M:%S"),
        'name': random.choice(['AAPL', 'AMZN', 'MSFT', 'GOOG', 'FB']),
        'tx_name': random.choice(['Barry', 'Edwin', 'Jack', 'Leo', 'Fred', 'Tony'])
    }


if __name__ == '__main__':
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    kinesis_client = session.client('kinesis', region_name=REGION)

    while True:
        interval1 = random.randint(0, 10) - 5
        data1 = get_price_data(interval1)
        interval2 = random.randint(0, 10) - 5
        data2 = get_tx_data(interval2)
        status1 = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(data1),
            PartitionKey="partition_key")
        print(status1)
        status2 = kinesis_client.put_record(
            StreamName=STREAM2_NAME,
            Data=json.dumps(data2),
            PartitionKey="partition_key")
        print(status2)
        time.sleep(SEND_INTERVAL * 0.001)
