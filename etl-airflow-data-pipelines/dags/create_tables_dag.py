from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from helpers.create_tables import create_all_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    #'start_date': datetime(2019, 1, 12),
    'start_date': datetime(2019, 9, 25, 0, 0, 0),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
}

dag = DAG('create_tables_dag',
          default_args=default_args,
          description='Create tables in Redshift using Airflow',
          #schedule_interval='0 * * * *',
          schedule_interval=None,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_all_tables = PostgresOperator(
    task_id="create_all_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_all_tables
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# task dependencies
start_operator >> create_all_tables
create_all_tables >> end_operator
