from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads data to the given fact table by running the provided sql statement.

    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param table: destination fact table on redshift.
    :type table: str
    :param columns: columns of the destination fact table
    :type columns: str containing column names in csv format.
    :param sql_stmt: sql statement to be executed.
    :type sql_stmt: str
    :param append: if False, a delete-insert is performed.
        if True, a append is performed.
        (default value: False)
    :type append: bool
    """
    ui_color = '#F98866'
    load_fact_sql = """
        INSERT INTO {} {} {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 columns="",
                 sql_stmt="",
                 append=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.sql_stmt = sql_stmt
        self.append = append

    def execute(self, context):
        self.log.info('LoadFactOperator has started')
        # obtain the redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)
        columns = "({})".format(self.columns)
        if self.append == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift_hook.run("DELETE FROM {}".format(self.table))
        load_sql = LoadFactOperator.load_fact_sql.format(
            self.table,
            columns,
            self.sql_stmt
        )
        redshift_hook.run(load_sql)
