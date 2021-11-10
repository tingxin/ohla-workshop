import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb',
                          region_name='ap-southeast-1',
                          aws_access_key_id="key id",
                          aws_secret_access_key="key secret"
                          )

table = dynamodb.Table('recommend-user-item')
response = table.query(
    KeyConditionExpression=Key('user_id').eq(str(180628000958))
)

if response['ResponseMetadata']['HTTPStatusCode'] == 200:
    if len(response['Items']) > 0:
        u = {
            'statusCode': 200,
            'body': response['Items']
        }
    else:
        u = {
            'statusCode': 404,
            'body': []
        }
else:
    u = {
        'statusCode': response['ResponseMetadata']['HTTPStatusCode'],
        'body': "error"
    }
print(u)
