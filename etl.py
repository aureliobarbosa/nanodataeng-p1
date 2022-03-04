import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function used to extract song data from the JSON file `filepath` and to load it into 
    a PostgreSQL database towards which the cursor `cur` is pointing to.

    Data stored on `filepath` is inserted on the tables `songs` and `artists` from the Sparkify database.
    """

    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id','year', 'duration' ]].values[0].tolist()        
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name','artist_location', 'artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Function used to extract log data from the JSON file `filepath` and to load it into 
    a PostgreSQL database towards which the cursor `cur` is pointing to.

    This functions is used to populate the tables `users`, `songplays`, and `times` from the Sparkify database.
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    filter = df['page']=='NextSong'
    df_time_table = df[filter]

    # convert timestamp column to datetime
    t = pd.to_datetime(df_time_table['ts'],  origin='unix', unit='ms')
    
    # insert time data records
    time_data = (t, 
                t.dt.hour, 
                t.dt.day, 
                t.dt.isocalendar().week, 
                t.dt.month, 
                t.dt.year, 
                t.dt.weekday, 
                )
    column_labels = ('timestamp', 'hour', 'day','weekofyear','month', 'year', 'weekday')

    dict_time_data = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(dict_time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    user_df.drop_duplicates(subset='userId',inplace=True)
    user_df.dropna(inplace=True)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_ts = pd.to_datetime(row.ts,  origin='unix', unit='ms')
        
        if row.userId:
            songplay_userId = row.userId
        else:
            songplay_userId = None

        songplay_data = (row.sessionId, 
                         songplay_ts,
                         songplay_userId, 
                         row.level,
                         songid,
                         artistid,
                         row.location,
                         row.userAgent,
        )
        
        if songplay_userId and songid and artistid:
            cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
    Sequencially read all JSON data files on the directory `filepath`, process it with function `func`, 
    and stores it's data on the PostgreSQL database pointed by the connector `conn` and cursor `cur`.
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, start=1):
        try:
            func(cur, datafile)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))
        except psycopg2.Error as e:
            print("Database Error! (psycopg2)." +
                   f"Error processing file '{datafile}' with function '{func.__name__}'.")
            print(e)
            conn.rollback()
            

def main():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error connecting to the database:\n", e)
      
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    conn.close()

if __name__ == "__main__":
    main()