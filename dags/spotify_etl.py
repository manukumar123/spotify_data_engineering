from pandas._libs.tslibs import timestamps
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, time
import datetime
import sqlite3
from keys import DATABASE_LOCATION, USER_ID, TOKEN


def check_if_valid(df: pd.DataFrame) -> bool:
    # Check if frame is empty
    if df.empty:
        print("No songs downloaded. Finishin execution")
        return False
    # played_at will be the primary key
    if not (pd.Series(df["played_at"]).is_unique):
        raise Exception("Error in primary key check")
    if df.isnull().values.any():
        raise Exception("Nulls detecetd!")
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #    # print(yesterday, timestamp)
    #    if datetime.datetime.strptime(timestamp, "%Y-%m-%d") not in [
    #        yesterday,
    #        datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    #    ]:
    #        raise Exception("Not all songs are from the last 24 hours")

    return True


def run_spotify_etl():

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN),
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(
            time=yesterday_unix_timestamp
        ),
        headers=headers,
    )
    data = r.json()

    song_name = []
    artist_name = []
    played_at = []
    timestamp = []

    for song in data["items"]:
        song_name.append(song["track"]["name"])
        artist_name.append(song["track"]["album"]["artists"][0]["name"])
        played_at.append(song["played_at"])
        timestamp.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_name,
        "artist_name": artist_name,
        "played_at": played_at,
        "timestamp": timestamp,
    }

    song_df = pd.DataFrame(song_dict)

    if check_if_valid(song_df):
        print("Data is valid!")

    engine = sqlalchemy.create_engine(DATABASE_LOCATION, pool_pre_ping=True)
    conn = sqlite3.connect("my_played_tracks.sqlite")
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened DB successfully!")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
    except:
        print("Data already exists!")

    conn.close()
    print("Closed DB successfully!")
