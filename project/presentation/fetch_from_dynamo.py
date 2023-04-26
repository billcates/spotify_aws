import json
import boto3
import os

n=os.environ["rank"]

dynamodb_client = boto3.client('dynamodb')

def lambda_handler(event, context):
    # TODO implement
    existing_tables = dynamodb_client.list_tables()['TableNames']
    table_name_check='spotify_db'
    if table_name_check in existing_tables:
        response = dynamodb_client.get_item(
        TableName='spotify_db',
        Key={
            'order': {'S': n}
            }
        )
        print(response['Item'])
        print("song_name is ", response['Item']['song_name']['S'])
