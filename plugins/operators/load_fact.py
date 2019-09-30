from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    #template_fields = ('columns',)
    load_fact_sql = """
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

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.columns = columns
        self.sql_stmt = sql_stmt

    def execute(self, context):
        self.log.info('LoadFactOperator has started')
        # obtain the redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)
        columns = "({})".format(self.columns)
        load_sql = LoadFactOperator.load_fact_sql.format(
            self.table,
            columns,
            self.sql_stmt
        )
        redshift_hook.run(load_sql)
