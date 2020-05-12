import pathlib

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

FILE = pathlib.Path(__file__).resolve().parent /  'create_tables.sql'

class CreateTableOperator(BaseOperator):

    """
    Loads sql create table queries from a file and runs them to create tables in redshift

   :param redshift_conn_id: Redshift connection id for access to redshift
   :param tables: A list of tables to be created
    """

    @apply_defaults
    def  __init__(self,
                redshift_conn_id="",
                tables=None,
                  *args,
                  **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        #load sql file
        with open(FILE, 'r') as f:
            sql_reads = f.read().strip().split(';')
            print('done')

        #process sql file
        sql_statements = [ query.replace('\n\t', "").replace('\n', "")\
                         for query in sql_reads ]


        for table in self.tables:
            redshift.run(f"DROP TABLE IF EXISTS {table}")
            self.log.info("Dropped {table} table Successfully")

        for query in sql_statements:
            if not query == "":
                self.log.info("query: "+query)
                redshift.run(query, autocommit=True)
        self.log.info('Created tables successfully')

