from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("S3_key", )
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 S3_bucket="",
                 S3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.S3_bucket = S3_bucket
        self.S3_key = S3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info('StageToRedshiftOperator implemented')





