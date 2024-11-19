import os
import json
import mysql.connector
import boto3
from chalice import Chalice

app = Chalice(app_name='backend')
app.debug = True

# s3 things
S3_BUCKET = 'huk5pd-dp1-spotify'
s3 = boto3.client('s3')

# base URL for accessing the files
baseurl = 'http://huk5pd-dp1-spotify.s3-website-us-east-1.amazonaws.com/'

# database things
DBHOST = os.getenv('DBHOST')
DBUSER = os.getenv('DBUSER')
DBPASS = os.getenv('DBPASS')
DB = os.getenv('DB')
db = mysql.connector.connect(user=DBUSER, host=DBHOST, password=DBPASS, database=DB)
cur = db.cursor()

# file extensions to trigger on
_SUPPORTED_EXTENSIONS = (
    '.json'
)

# ingestor lambda function
@app.on_s3_event(bucket=S3_BUCKET, events=['s3:ObjectCreated:*'])
def s3_handler(event):
    if _is_json(event.key):
        # Get the file from S3 and parse its contents
        response = s3.get_object(Bucket=S3_BUCKET, Key=event.key)
        text = response["Body"].read().decode()
        data = json.loads(text)

        # Parse the metadata fields from the JSON object
        TITLE = data.get('title', 'Unknown Title')
        ALBUM = data.get('album', 'Unknown Album')
        ARTIST = data.get('artist', 'Unknown Artist')
        YEAR = data.get('year', 'Unknown Year')
        GENRE = data.get('genre', 'Unknown Genre')

        # Generate the unique ID for URLs
        keyhead = event.key
        identifier = keyhead.split('.')[0]
        ID = identifier
        MP3 = baseurl + ID + '.mp3'
        IMG = baseurl + ID + '.jpg'

        app.log.debug("Received new song: %s, key: %s", event.bucket, event.key)

        # Try to insert the song into the database
        try:
            add_song = ("INSERT INTO songs "
                        "(title, album, artist, year, file, image, genre) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)")
            song_vals = (TITLE, ALBUM, ARTIST, YEAR, MP3, IMG, GENRE)
            cur.execute(add_song, song_vals)
            db.commit()

        except mysql.connector.Error as err:
            app.log.error("Failed to insert song: %s", err)
            db.rollback()

# Perform a suffix match against supported extensions
def _is_json(key):
    return key.endswith(_SUPPORTED_EXTENSIONS)
