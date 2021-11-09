import boto3
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    user_id = event["user_id"]

    dynamodb = boto3.resource('dynamodb', region_name='cn-northwest-1')
    table = dynamodb.Table('demo-ohla-recommand')
    response = table.query(
        KeyConditionExpression=Key('user_id').eq(str(user_id))
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
    return u
