# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# fact table
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id serial, \
                                                                    start_time time, \
                                                                    user_id integer, \
                                                                    level varchar, \
                                                                    song_id integer, \
                                                                    artist_id integer, \
                                                                    session_id integer, \
                                                                    location varchar, \
                                                                    user_agent varchar);  
                                                                    """)

# dimensions
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id integer, \
                                                                    first_name varchar, \
                                                                    last_name varchar, \
                                                                    gender char(1), \
                                                                    level varchar);  
                                                                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar, \
                                                                    title varchar, \
                                                                    artist_id varchar, \
                                                                    year integer, \
                                                                    duration numeric(5,2));  
                                                                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar, \
                                                                    name varchar, \
                                                                    location varchar, \
                                                                    latitude numeric(5,2), \
                                                                    longitude numeric(5,2));  
                                                                    """)


time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time time, \
                                                                    hour integer, \
                                                                    day integer, \
                                                                    week integer, \
                                                                    month integer, \
                                                                    year integer, \
                                                                    weekday integer);  
                                                                    """)

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, \
                                                    session_id, location, user_agent) \
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
                 VALUES (%s, %s, %s, %s, %s)""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
                 VALUES (%s, %s, %s, %s, %s)""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                 VALUES (%s, %s, %s, %s, %s)""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                 VALUES (%s, %s, %s, %s, %s, %s, %s)""")

# FIND SONGS

song_select = ("""SELECT song_id, artists.artist_id \
                    from songs JOIN artists ON songs.artist_id = artists.artist_id \
                    where title = %s and name = %s and duration = %s; """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]