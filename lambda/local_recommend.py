import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb',
                          region_name='cn-northwest-1',
                          aws_access_key_id="AKIAQMS6D5EI3FOTFUE4",
                          aws_secret_access_key="your key"
                          )

table = dynamodb.Table('demo-ohla-recommand')
response = table.query(
    KeyConditionExpression=Key('user_id').eq(str(18953))
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
