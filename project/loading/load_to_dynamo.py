import boto3
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
client = boto3.client('dynamodb')


existing_tables = client.list_tables()['TableNames']
table_name_check='spotify_db'
if table_name_check in existing_tables:
    client.delete_table(TableName='spotify_db')
    waiter = client.get_waiter('table_not_exists')
    waiter.wait(TableName='spotify_db')
    print("table deltetd")

table1 = dynamodb.create_table(
    TableName='spotify_db',
    KeySchema=[
        {
            'AttributeName': 'song_name',
            'KeyType': 'HASH'  #Partition key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'song_name',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)
table = dynamodb.Table("spotify_db")

print("table created")

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']
    resp = s3_client.get_object(Bucket=bucket_name,Key=s3_file_name)
    data = resp['Body'].read().decode("utf-8")
    Songs = data.split("\n")
    #print(friends)
    for song in Songs:
        print(song)
        song_data = song.split(",")
        # add to dynamodb
        try:
            table.put_item(
                Item = {
                    "order" : song_data[0],
                    "song_name" : song_data[1],
                    "artist_name"      : song_data[2],
                    "album_name"   : song_data[3],
                    "played_at"   : song_data[4],
                    "duration"   : song_data[5]
                }
            )
        except Exception as e:
            print("End of file")