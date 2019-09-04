import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh-script.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# staging table schema based on the json content
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                  artist text, 
                                  auth text, 
                                  firstName varchar, 
                                  gender varchar(6), 
                                  itemInSession varchar,
                                  lastName varchar, 
                                  length float, 
                                  level varchar(10), 
                                  location text,
                                  method varchar(10),
                                  page varchar(20),
                                  registration bigint, 
                                  sessionId int,
                                  song text,
                                  status smallint,
                                  ts bigint,
                                  userAgent text,
                                  userId int)
                                  ;"""
                             )

# staging table schema based on the json content
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                  num_songs integer,
                                  artist_id text,
                                  artist_latitude numeric(15,2),
                                  artist_longitude numeric(15,2),
                                  artist_location text,
                                  artist_name text,
                                  song_id text,
                                  title text,
                                  duration numeric(15,2),
                                  year integer)
                                  ;"""
                              )

# fact table with references to dimension table keys
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1) PRIMARY KEY, \
                                                                    start_time timestamp REFERENCES time, \
                                                                    user_id int REFERENCES users, \
                                                                    song_id varchar REFERENCES songs, \
                                                                    artist_id varchar REFERENCES artists, \
                                                                    session_id int, \
                                                                    location varchar, \
                                                                    user_agent varchar, \
                                                                    level varchar);  
                                                                    """)

# dimensions
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id integer PRIMARY KEY, \
                                                                    first_name varchar NOT NULL, \
                                                                    last_name varchar NOT NULL, \
                                                                    gender varchar(6), \
                                                                    level varchar NOT NULL);  
                                                                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, \
                                                                    title varchar NOT NULL, \
                                                                    artist_id varchar, \
                                                                    year int, \
                                                                    duration float);  
                                                                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, \
                                                                    name varchar NOT NULL, \
                                                                    location varchar, \
                                                                    latitude numeric(15,2), \
                                                                    longitude numeric(15,2));  
                                                                    """)


time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, \
                                                                    hour integer, \
                                                                    day integer, \
                                                                    week integer, \
                                                                    month integer, \
                                                                    year integer, \
                                                                    weekday integer);  
                                                                    """)

# STAGING TABLES

# get data from s3 into staging_events, notice we need specify json format.
staging_events_copy = ("""copy staging_events from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' 
    format as json {} 
    truncatecolumns;
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

# get data from s3 into staging_songs, this takes ~30 mins on 8 node cluster
staging_songs_copy = ("""copy staging_songs from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' 
    format as json 'auto' 
    truncatecolumns;
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# SQL-to-SQL ETL TABLES

# select data from staging_events joined with another joined artist and songs table
songplay_table_insert = ("""INSERT into songplays (start_time, user_id, song_id, artist_id,
                                                    session_id, location, user_agent, level)
                                SELECT (TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as start_time,
                                        se.userid,
                                        joined_song_artist_table.song_id,
                                        joined_song_artist_table.artist_id, 
                                        se.sessionid,
                                        se.location,
                                        se.useragent,
                                        se.level
                                FROM staging_events as se
                                JOIN (
                                        SELECT s.song_id,
                                               a.artist_id,
                                               s.title,
                                               a.name,
                                               s.duration
                                        FROM songs as s
                                        JOIN artists as a
                                        ON s.artist_id = a.artist_id
                                     ) as joined_song_artist_table
                                     
                                ON se.song = joined_song_artist_table.title and
                                   se.artist = joined_song_artist_table.name and
                                   round(se.length,2) = joined_song_artist_table.duration;
                        """)

# when multiple user_ids are present, get the record with latest timestamp
user_table_insert = ("""INSERT into users (user_id, first_name, last_name, gender, level) \
                            SELECT se.userid, se.firstname, se.lastname, se.gender, se.level
                                FROM staging_events as se
                            JOIN
                                (SELECT userid, MAX(ts) as max_ts
                                    FROM staging_events
                                    WHERE page='NextSong'
                                    GROUP BY userid
                                ) as latest
                            ON se.userid = latest.userid and 
                               se.ts = latest.max_ts;
                    """)

# select distinct song ids and insert them into songs
song_table_insert = ("""INSERT into songs (song_id, title, artist_id, year, duration)
                            SELECT distinct(song_id), title, artist_id, year, duration
                            FROM staging_songs;
                    """)

# select distinct artist ids and insert them into artists
artist_table_insert = ("""INSERT into artists(artist_id, name, location, latitude, longitude)
                            SELECT distinct(artist_id), artist_name, artist_location, \
                            artist_latitude, artist_longitude
                            FROM staging_songs;
                       """)

# convert the epoch time to a timestamp and extract time attributes
time_table_insert = ("""INSERT into time (start_time, hour, day, week, month, year, weekday)
                        SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                            EXTRACT(hour from start_time) as hour,
                            EXTRACT(day from start_time) as day,
                            EXTRACT(week from start_time) as week,
                            EXTRACT(month from start_time) as month,
                            EXTRACT(year from start_time) as year,
                            EXTRACT(dow from start_time) as weekday    
                        FROM staging_events;
                     """)

# QUERY LISTS
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_songs_table_drop, staging_events_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
