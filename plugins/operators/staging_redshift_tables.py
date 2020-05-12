from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import  BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageTablesToRedshiftOperator(BaseOperator):
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        BLANKSASNULL EMPTYASNULL
        TIMEFORMAT as 'epochmillisecs'
        FORMAT as JSON '{}'
    """
    @apply_defaults
    def  __init__(self,
                  redshift_conn_id="",
                  aws_credentials_id="",
                  table="",
                  s3_bucket="",
                  s3_key="",
                  region="",
                  file_format="",
                  *args,
                  **kwargs):

            super(StageTablesToRedshiftOperator, self).__init__(*args, **kwargs)
            self.table = table
            self.redshift_conn_id = redshift_conn_id
            self.s3_bucket = s3_bucket
            self.s3_key = s3_key
            self.aws_credentials_id = aws_credentials_id
            self.region = region
            self.file_format = file_format
            self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        s3_path =  f"s3://{self.s3_bucket}/{self.s3_key}/"
        self.log.info('Clearing data from destination Redshift table')
        redshift.run(f"DELETE FROM {self.table}")

        if self.execution_date:
            year = self.execution_date.year
            month = self.execution_date.month
            day = self.execution_date.day

            s3_path = f"s3://{self.s3_bucket}/{self.s3_key}/{year}/{month}/{day}/{self.s3_key}"


        formatted_sql_timeformat = StageTablesToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format
        )

        redshift.run(formatted_sql_timeformat)
        self.log.info(f"Data Loaded data into {self.table}")
