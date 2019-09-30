import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, SongPopularityOperator,
                                UnloadToS3Operator)
from subdag import perform_data_quality_checks
from helpers import SqlQueries
from helpers.data_quality_checks import DataQualityChecks

# Default arguments for DAG and SubDag:
# start_date : since we only have data for the month of Nov'18
# end_data   : since we only have data for the month of Nov'18
# depends_on_past : this DAG is independent of previous runs.
# email_on_retry : We don't want emails on retry.
# retries : Number of times the task is retries upon failure.
# retry_delay : How much time should the scheduler wait before re-attempt.
# catchup_by_default : scheduler will create a DAG run for any
#   interval that has not been run.
#   NOTE:some blogs say that this doesn't work in default_args and
#   we should set it explicitly on the dag.
# provide_context : When you provide_context=True to an operator, we pass
#   along the Airflow context variables to be used inside the operator.
#
default_args = {
    'owner': 'shravan',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'provide_context': True,
}

dag = DAG(
        'load_and_transform_data_in_redshift',
        default_args=default_args,
        description='Load and Transform data in Redshift using Airflow',
        schedule_interval='@daily',
        #catchup=False,
        max_active_runs=1
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Since we have provided context = True in the default args, we have access to
# {execution_date.year} and {execution_date.month} and {ds}
# On S3, the data is partitioned in this format:
# s3://udacity-dend/log_data/2018/11/2018-11-01-events.json
# s3://udacity-dend/log_data/2018/11/2018-11-30-events.json
# Finally, we also pass in the json_format
copy_events_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_events_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    arn_iam_role = "arn:aws:iam::506140549518:role/dwhRole",
    region = "us-west-2",
    json_format = "s3://udacity-dend/log_json_path.json"
)

copy_songs_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_songs_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_songs",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    arn_iam_role = "arn:aws:iam::506140549518:role/dwhRole",
    region = "us-west-2",
    json_format = "auto"
)

load_fact_dimension = LoadFactOperator(
    task_id="load_songplays_fact_table",
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    columns ="""
        songplay_id,
        start_time,
        userid,
        level,
        songid,
        artistid,
        sessionid,
        location,
        user_agent
    """,
    sql_stmt = SqlQueries.songplay_table_insert,
    append=True
)

load_user_dimension = LoadDimensionOperator(
    task_id="load_user_dim_table",
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    columns = """
        userid,
        first_name,
        last_name,
        gender,
        level
    """,
    sql_stmt = SqlQueries.user_table_insert,
    append=False
)

load_song_dimension = LoadDimensionOperator(
    task_id="load_song_dim_table",
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songs",
    columns ="""
        songid,
        title,
        artistid,
        year,
        duration
    """,
    sql_stmt = SqlQueries.song_table_insert,
    append=False
)

load_artist_dimension = LoadDimensionOperator(
    task_id="load_artist_dim_table",
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    columns ="""
        artistid,
        name,
        location,
        lattitude,
        longitude
    """,
    sql_stmt = SqlQueries.artist_table_insert,
    append=False
)

load_time_dimension = LoadDimensionOperator(
    task_id="load_time_dim_table",
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time",
    columns ="""
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    """,
    sql_stmt = SqlQueries.time_table_insert,
    append=False
)

def run_data_quality_check(table, query, default_args=default_args, dag=dag):
    """
        SubDag definition
    """
    task_id = "{}_data_quality_subdag".format(table)
    data_quality_subdag_task = SubDagOperator(
        subdag=perform_data_quality_checks(
            "load_and_transform_data_in_redshift",
            task_id,
            "redshift",
            table,
            default_args,
            query,
        ),
        task_id = task_id,
        dag=dag,
    )
    return data_quality_subdag_task

# use a dictionary comprehension on the returned dictionary
# tables = {
#    "artists": DataQualityChecks.artists_table_check,
#    "songs": DataQualityChecks.songs_table_check,
#    "time": DataQualityChecks.time_table_check,
#    "users": DataQualityChecks.users_table_check,
#    "songplays": DataQualityChecks.songplays_table_check
#  }
data_quality_subdags = {table:run_data_quality_check(table, query)
    for table, query in DataQualityChecks.get_data_quality_checks().items()}

calculate_song_popularity = SongPopularityOperator(
    task_id="calculate_song_popularity",
    dag=dag,
    redshift_conn_id = "redshift",
    destination_table = "songpopularity",
    origin_table = "songplays",
    origin_dim_table = "songs",
    groupby_column = "title",
    fact_column = "duration",
    join_column = "songid"
)

unload_to_s3 = UnloadToS3Operator(
    task_id="unload_to_s3",
    dag=dag,
    redshift_conn_id = "redshift",
    source_table = "songpopularity",
    s3_bucket = "skuchkula-topsongs",
    s3_key = "songpopularity_{ds}",
    arn_iam_role = "arn:aws:iam::506140549518:role/dwhRole"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> copy_events_from_s3_to_redshift
start_operator >> copy_songs_from_s3_to_redshift
copy_songs_from_s3_to_redshift >> load_fact_dimension
copy_events_from_s3_to_redshift >> load_fact_dimension
load_fact_dimension >> [load_user_dimension, load_song_dimension,
                        load_artist_dimension, load_time_dimension]

load_user_dimension >> data_quality_subdags['users']
load_song_dimension >> data_quality_subdags['songs']
load_artist_dimension >> data_quality_subdags['artists']
load_time_dimension >> data_quality_subdags['time']
load_fact_dimension >> data_quality_subdags['songplays']

for subdag in data_quality_subdags.values():
    subdag >> calculate_song_popularity

calculate_song_popularity >> unload_to_s3
unload_to_s3 >> end_operator
