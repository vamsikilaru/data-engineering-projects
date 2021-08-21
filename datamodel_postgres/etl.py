import os
import glob
from pandas.core.indexes.base import Index
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

def process_log_file(cur,filepath):
    # open log
    df = pd.read_json(filepath,lines=True)  
    # filter by next song action     
    df = df[df['page']=="NextSong"].astype({'ts':'datetime64[ms]'})
    #convert timestamp to datetime
    t = pd.Series(df['ts'],index=df.index)
    #insert time data records
    column_lables = ["timestamp","hour","day","weekofyear","month","year","weekday"]
    time_data =[]
    for data in t:
        time_data.append([data,data.hour,data.day,data.weekofyear,data.month,data.year,data.day_name()])
    time_df = pd.DataFrame.from_records(data=time_data,columns=column_lables)

    for i,row in time_df.iterrows():
        cur.execute(time_table_insert,list(row))
    # user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert,row)
    # insert song play records
    for index,row in df.iterrows():
        #get song play record from song_id and artist_id 
        cur.execute(song_select,(row.song,row.artist,row.length))
        result = cur.fetchone()
        if result:
            songid,artistid = result
        else:
            songid,artistid = None, None
        #prepare song data
        songplay_data = (row.ts, row.userId, row.level, songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert,songplay_data)

def process_data(cur,conn,filepath,func):

    all_files = []
    for root,dirs,files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    # total number of files
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files,filepath))
    #iterate over files and process
    for i,datafile in enumerate(all_files,1):
        func(cur,datafile)
        conn.commit()
        print('{}/{} files processed'.format(i,num_files))

def main():
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=password")
    cur =conn.cursor()
    process_data(cur,conn,filepath='data/song_data',func=process_song_file)
    process_data(cur,conn,filepath='data/log_data',func=process_log_file)
    cur.close()
    conn.close()

if __name__=="__main__":
    main()
    print("\n\n DATA PROCESS COMPLETED!! \n\n")