from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Stages data to a specific redshift cluster from a specified S3 location.

    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param aws_credentials: reference to a aws hook containing iam details
    :type aws_credentials: str
    :param table: destination staging table on redshift.
    :type table: str
    :param s3_bucket: source s3 bucket name
    :type s3_bucket: str
    :param s3_key: source s3 prefix (templated)
    :type s3_key: Can receive a str representing a prefix,
        the prefix can contain a path that is partitioned by some field.
    :param arn_iam_role: iam role which has permission to read data from s3
    :type arn_iam_role: str
    :param region: aws region where the redshift cluster is located.
    :type region: str
    :param json_format: source json format
    :type json_format: str
    """
    ui_color = '#358140'
    # use a templated field - a fancy term for templated(formated) strings
    # s3_key will be : song_data (or)
    # log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json
    template_fields = ("s3_key",)
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
        redshift_hook.run("truncate {}".format(self.table))

        # as we are providing_context = True, we get them in kwargs form
        # use **context to upack the dictionary and format the s3_key
        rendered_key = self.s3_key.format(**context)

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        sql_stmt = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.arn_iam_role,
            self.region,
            self.json_format
        )
        self.log.info(f"Running COPY SQL: {sql_stmt}")
        redshift_hook.run(sql_stmt)
