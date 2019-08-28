import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Use this filepath to get the data to run song data processing. 
    Parameters:
        cur  : The cursor that will be used to execute queries.
        filepath : The JSON file path resides song data.
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', \
                  'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Use this filepath to get the data to run log data processing. 
    Parameters:
        cur  : The cursor that will be used to execute queries.
        filepath : The JSON file path resides log data.
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = df.ts.apply(lambda x: pd.to_datetime(x, unit='ms'))
    
    # insert time data records
    time_data = [t.dt.time, t.dt.hour, t.dt.day, t.dt.weekofyear, \
             t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['time', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    # sk: round the duration to two decimal places
    df['length'] = df.length.apply(lambda x: round(x, 2))
    df['ts'] = df.ts.apply(lambda x: pd.to_datetime(x, unit='ms'))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, \
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Use this filepath to get the data to run song data processing. 
    Parameters:
        cur  : The cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
        filepath : The file path of data resides.
        func : Defined the function to run through this file path of data.
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
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
