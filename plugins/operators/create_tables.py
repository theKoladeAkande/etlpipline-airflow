from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    create_table_file='create_tables.sql'


    @apply_defaults
    def  __init__(self,
                redshift_conn_id="",
                  file=None,
                  *args,
                  **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        #load sql file
        with open(CreateTableOperator.create_table_file, 'r') as f:
            sql_reads = f.read().strip().split(';')

        #process sql file
        sql_statements = [ query.replace('\n\t', "").replace('\n', "")\
                         for query in sql_reads ]

        self.log.info("Creating tables in redshift")
        for query in sql_statements:
            if not query == "":
                self.log.info("query: "+query)
                redshift.run(query, autocommit=True)
