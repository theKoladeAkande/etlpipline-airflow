from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

class LoadFactOperator(BaseOperator):
    """
    Runs sql queries for loading data into a fact table in redshift

    :param redshift_conn_id: Redshift connection id for access to redshift
    :param table: table name
    :param sql_query: sql query to perform transform and load operation
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Loading data into the fact table:{self.table}')
        redshift.run(self.sql_query)
