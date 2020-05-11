import datetime.datetime
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
}



#create DAG
dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description= 'Performs ETL operations form S3 to Redshift' )



start_operator =  DummyOperator(task_id='Begin_execution', dag=dag)

create_stage_songs_table = PostgresOperator(
                            task_id='create_stage_songs_table',
                            dag=dag,
                            postgres_conn_id="redshift",
                            sql= SqlQueries.create_stage_songs_table
)

create_stage_events_table = PostgresOperator(
                            task_id='create_stage_events_table',
                            dag=dag,
                            postgres_conn_id="redshift",
                            sql= SqlQueries.create_stage_events_table
)