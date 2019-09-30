import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Performs data quality checks by running sql statements to validate the data.

    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param table: destination fact table on redshift.
    :type table: str
    :param query: sql statement to validate the data.
    :type query: str
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        self.log.info(f'Checking DataQuality for {self.table}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Running query: {self.query}')
        records = redshift_hook.get_records(self.query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records != 0:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f'PASSED DATA QUALITY CHECK!! for {self.table}')
