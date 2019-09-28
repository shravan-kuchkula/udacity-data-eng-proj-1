#Instructions
#In this exercise, we’ll place our S3 to RedShift Copy operations into a SubDag.
#1 - Consolidate HasRowsOperator into the SubDag
#2 - Reorder the tasks to take advantage of the SubDag Operators

import datetime

from airflow import DAG
from airflow.operators import HasRowsOperator
from airflow.operators import DataQualityOperator


# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present.
def perform_data_quality_checks(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        default_args,
        check_sql_stmt,
        *args, **kwargs):

    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        default_args=default_args,
        schedule_interval=None,
        **kwargs
    )

    check_has_rows_task = HasRowsOperator(
        task_id = f"check_has_rows_for_{table}",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id
    )

    check_data_quality_task = DataQualityOperator(
        task_id = f"perform_data_quality_checks_for_{table}",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        query=check_sql_stmt
    )

    check_has_rows_task >> check_data_quality_task

    return dag
