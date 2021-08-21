import os
import glob
import psycopg2
import pandas as pd
from sql_queries  import *

def process_song_file(cur,filepath):

    df = pd.DataFrame([pd.read_json(filepath,typ='series',convert_dates=False)])
    for value in df.values:
        num_songs,artist_id,artist_latitude,artist_longitude,artist_location,artist_name,song_id,title,duration,year = value
        # insert artist record
        artist_data = (artist_id,artist_name,artist_location,artist_latitude,artist_longitude)
        cur.execute(artist_table_insert,artist_data)
        # insert song record
        song_data = (song_id,title,artist_id,year,duration)
        cur.execute(song_table_insert,song_data)
    print(f"Records inserted for file : {filepath}")

        
