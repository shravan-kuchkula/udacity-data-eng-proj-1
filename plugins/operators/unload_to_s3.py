import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UnloadToS3Operator(BaseOperator):
    """
    Stores the analysis result back to the given S3 location.

    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param source_table: analyis table on which the query is executed
    :type table: str
    :param s3_bucket: source s3 bucket name
    :type s3_bucket: str
    :param s3_key: source s3 prefix (templated)
    :type s3_key: Can receive a str representing a prefix,
        the prefix can contain a path that is partitioned by some field.
    :param arn_iam_role: iam role which has permission to read data from s3
    :type arn_iam_role: str
    """

    ui_color = '#0085b6'
    template_fields = ("s3_key",)
    unload_sql_template = """
    unload('select * from {}')
    to '{}'
    iam_role '{}'
    parallel off;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 source_table="",
                 s3_bucket="",
                 s3_key="",
                 arn_iam_role="",
                 *args, **kwargs):

        super(UnloadToS3Operator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.source_table = source_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.arn_iam_role = arn_iam_role

    def execute(self, context):
        self.log.info('UnloadToS3Operator is now in progress')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_location = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        unload_sql = UnloadToS3Operator.unload_sql_template.format(
            self.source_table,
            s3_location,
            self.arn_iam_role
        )
        logging.info("Unloading to S3: {}".format(unload_sql))
        redshift.run(unload_sql)
