import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pandas as pd
from schema import *


config = configparser.ConfigParser()

#Normally this file should be in ~/.aws/credentials
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Function to create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    The function to process song data
    
    Parameters:
        spark  : The Spark session that will be used to execute commands.
        input_data : The input data to be processed.
        output_data : The location where to store the parquet tables.
    """
    # get filepath to song data file
    song_data = input_data
    
    # Read in the data file
    df_song = spark.read.json(song_data)
    
    # Extract columns to create songs table
    song_cols = [song_id, title, artist_id, year, duration]
    
    # Eliminate duplicate (if any) by selecting distinct song_id's
    # groupby song_id and select the first record's title in the group.
    t1 = df_song.select(F.col('song_id'), 'title') \
        .groupBy('song_id') \
        .agg({'title': 'first'}) \
        .withColumnRenamed('first(title)', 'title1')

    # right table containing all the columns
    t2 = df_song.select(song_cols)
    
    # join on title and select the song columns
    song_table_df = t1.join(t2, 'song_id') \
                .where(F.col("title1") == F.col("title")) \
                .select(song_cols)
    
    # Write songs table to parquet files partitioned by year and artist
    song_table_df.write.parquet(output_data + 'songs_table', 
                                partitionBy=['year', 'artist_id'], 
                                mode='Overwrite')

    # Extract columns to create artists table
    artists_cols = [artist_id, name, location, latitude, longitude]
    
    # Eliminate duplicates (if any) by selecting distinct artist_ids
    # groupby artist_id and select the first record's artist_name in the group.
    t1 = df_song.select(F.col('artist_id'), 'artist_name') \
        .groupBy('artist_id') \
        .agg({'artist_name': 'first'}) \
        .withColumnRenamed('first(artist_name)', 'artist_name1')

    # right table containing all the columns
    t2 = df_song.select(artists_cols)
    
    # join on artist_name and select the artist columns
    artists_table_df = t1.join(t2, 'artist_id') \
                .where(F.col("artist_name1") == F.col("artist_name")) \
                .select(artists_cols)
    
    # write artists table to parquet files
    artists_table_df.write.parquet(output_data + 'artists_table', mode='Overwrite')


def process_log_data(spark, input_data, output_data):
    """
    The function to process song data
    
    Parameters:
        spark  : The Spark session that will be used to execute commands.
        input_data : The input data to be processed.
        output_data : The location where to store the parquet tables.
    """
    # get filepath to log data file
    log_data = input_data
    
    # read log data file
    df_log = spark.read.json(input_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(F.col("page") == "NextSong")

    # Extract columns for users table
    users_cols = [user_id, first_name, last_name, gender, level]
    
    # remove duplicate rows
    users_table_df = df_log.select(users_cols).dropDuplicates()
    
    # write users table to parquet files
    users_table_df.write.parquet(output_data + 'users_table', mode='Overwrite')

    # define functions for extracting time components from ts field
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    get_hour = F.udf(lambda x: x.hour, T.IntegerType()) 
    get_day = F.udf(lambda x: x.day, T.IntegerType()) 
    get_week = F.udf(lambda x: x.isocalendar()[1], T.IntegerType()) 
    get_month = F.udf(lambda x: x.month, T.IntegerType()) 
    get_year = F.udf(lambda x: x.year, T.IntegerType()) 
    get_weekday = F.udf(lambda x: x.weekday(), T.IntegerType()) 
    
    # create timestamp column from original timestamp column
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    df_log = df_log.withColumn("hour", get_hour(df_log.timestamp))
    df_log = df_log.withColumn("day", get_day(df_log.timestamp))
    df_log = df_log.withColumn("week", get_week(df_log.timestamp))
    df_log = df_log.withColumn("month", get_month(df_log.timestamp))
    df_log = df_log.withColumn("year", get_year(df_log.timestamp))
    df_log = df_log.withColumn("weekday", get_weekday(df_log.timestamp))
    
    # extract columns to create time table
    time_cols = [start_time, hour, day, week, month, year, weekday]
    time_table_df = df_log.select(time_cols)
    
    # write time table to parquet files partitioned by year and month
    time_table_df.write.parquet(output_data + 'time_table', 
                                partitionBy=['year', 'month'], 
                                mode='Overwrite')

    # read in song data to use for songplays table
    # read the partitioned data
    df_artists_read = spark.read.option("mergeSchema", "true").parquet(output_data + "artists_table")
    df_songs_read = spark.read.option("mergeSchema", "true").parquet(output_data + "songs_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplay_cols = [start_time, user_id, song_id, artist_id, session_id, 
                     locationSP, user_agent, level, month, year]
    
    # join artists and songs so that we can join this table in the next step
    df_joined_songs_artists = df_songs_read.join(df_artists_read, 'artist_id').select("artist_id", "song_id",
                                                                                      "title", "artist_name")
    # join df_log with the earlier joined artist and songs table
    songplay_table_df = df_log.join(df_joined_songs_artists, 
                                    df_log.artist == df_joined_songs_artists.artist_name).select(songplay_cols)
    
    # create songplay_id
    songplay_table_df = songplay_table_df.withColumn("songplay_id", 
                                                     F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplay_table_df.write.parquet(output_data + 'songplays_table', 
                                    partitionBy=['year', 'month'], 
                                    mode='Overwrite')
    

def main():
    spark = create_spark_session()
    songPath = 'data/song_data/*/*/*/*.json'
    logPath = 'data/log_data/*.json'
    output_data = 'spark-warehouse/'
    
    process_song_data(spark, songPath, output_data)    
    process_log_data(spark, logPath, output_data)


if __name__ == "__main__":
    main()
