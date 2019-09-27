from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_dimension_sql = """
        INSERT INTO {} {} {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 columns="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.columns = columns
        self.sql_stmt = sql_stmt

    def execute(self, context):
        self.log.info('LoadDimensionOperator has started')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        columns = "({})".format(self.columns)
        load_sql = LoadDimensionOperator.load_dimension_sql.format(
            self.table,
            columns,
            self.sql_stmt
        )
        redshift_hook.run(load_sql)
