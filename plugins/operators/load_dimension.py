from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class LoadDimensionOperator(BaseOperator):
    """
    Runs query to load data into target dimension table in redshift
    :param redshift_conn_id: Redshift connection id for access to redshift
    :param table: table name
    :param clear_previous: delete previous table entries
    :param sql_query: sql query to perform transform and load operation
    """


    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                clear_previous="",
                sql_query="",
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.clear_previous =  clear_previous
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.clear_previous:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run(f" TRUNCATE   TABLE {self.table}")

        redshift.run(self.sql_query)
        self.log.info(f'Loaded data into table: {self.table}')