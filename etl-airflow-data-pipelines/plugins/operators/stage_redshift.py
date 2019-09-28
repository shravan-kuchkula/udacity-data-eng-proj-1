from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        compupdate off
        REGION '{}'
        FORMAT AS JSON '{}'
        truncatecolumns;
    """


    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 arn_iam_role="",
                 region="",
                 json_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.arn_iam_role = arn_iam_role
        self.region = region
        self.json_format = json_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is now in progress')

        # get the hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        sql_stmt = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.arn_iam_role,
            self.region,
            self.json_format
        )
        self.log.info(f"Running COPY SQL: {sql_stmt}")
        redshift_hook.run(sql_stmt)
