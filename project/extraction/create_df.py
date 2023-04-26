import requests
import pandas as pd
import datetime
import yaml
import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyOAuth
from io import StringIO # python3; python2: BytesIO 
import boto3


with open('config.yaml') as f:
    fl=yaml.safe_load(f)
    cfg=fl['param']

client_id=cfg['client_id']
client_secret=cfg['client_secret']


scope = "user-read-recently-played"
redirect_url='http://localhost:8888/callback'

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id, client_secret= client_secret, redirect_uri=redirect_url, scope=scope))

results = sp.current_user_recently_played()
song_names=[]
artist_names=[]
album_names=[]
played_at=[]
duration_list=[]

for idx, item in enumerate(results['items']):
    track = item['track']
    #print(track)
    song_names.append(track['name'])
    artist_names.append(track['artists'][0]['name'])
    album_names.append(track['album']['name'])
    played_at.append(item["played_at"])
    duration_list.append(track['duration_ms'])
    rank=[i for i in range(1,51)]

song_dict = {
    "order" : rank,
    "song_name" : song_names,
    "artist_name": artist_names,
    "album_name" : album_names,
    "played_at" : played_at,
    "duration" : duration_list
}
song_df = pd.DataFrame(song_dict, columns = ["order","song_name", "artist_name", "album_name","played_at","duration"])


bucket = 'aws-spotify-data' # already created on S3
csv_buffer = StringIO()
song_df.to_csv(csv_buffer,index=False)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'input/df.csv').put(Body=csv_buffer.getvalue())

