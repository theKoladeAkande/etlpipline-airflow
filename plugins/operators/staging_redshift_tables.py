from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import  BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageTablesToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key")
    copy_sql_time_format = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS {}
        TIMEFORMAT AS '{}'
    """

    copy_sql="""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS {}
    """

    @apply_defaults
    def  __init__(self,
                  redshift_conn_id="",
                  aws_credentials_id="",
                  table="",
                  s3_bucket="",
                  s3_key="",
                  json="auto",
                  time_format=None,
                  *args,
                  **kwargs):

            super(StageTablesToRedshiftOperator, self).__init__(*args, *kwargs)
            self.table = table
            self.redshift_conn_id = redshift_conn_id
            self.s3_bucket = s3_bucket
            self.s3_key = s3_key
            self.aws_credentials_id = aws_credentials_id
            self.json = json
            self.time_format = time_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        s3_path =  f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info('Clearing data from destination Redshift table')
        redshift.run(f"DELETE FROM {self.table}")

        formatted_sql_timeformat = StageTablesToRedshiftOperator.copy_sql_time_format.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json,
            self.time_format
        )

        formatted_sql = StageTablesToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json,
        )

        self.log.info(f"loading data into staging table: {self.table}")

        if self.time_format:
            redshift.run(formatted_sql)
        else:
            redshift.run(formatted_sql_timeformat)