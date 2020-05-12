from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (DataQualityOperator,
                               StageTablesToRedshiftOperator,
                               LoadDimensionOperator,
                               LoadFactOperator,
                               CreateTableOperator)

from helpers.sql_queries import SqlQueries


default_args = {
    'owner': 'kolade',
    'start_date': datetime(2020, 5, 11),
    'end_date': datetime(2020, 12, 30),
}


#create DAG
dag = DAG('sparkify_etl_dag',
          description='Performs ETL operations form S3 to Redshift',
          max_active_runs=3,
          start_date=datetime(2020, 6, 10, 0,0,0,0)
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables_task = CreateTableOperator(
    task_id='create_tables',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["artists", "songplays", "songs", 
            "staging_events", "staging_songs",
           "time", "users"]
)

stage_events_task = StageTablesToRedshiftOperator(
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    file_format="s3://udacity-dend/log_json_path.json",
    provide_context=True,
    execution_date=None,
    task_id='staging_events_data',
    dag=dag
)
stage_songs_task = StageTablesToRedshiftOperator(
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    file_format='auto',
    task_id="staging_songs_data",
    provide_context=True,
    dag=dag
)

load_songs_play_fact_table = LoadFactOperator(
    redshift_conn_id='redshift',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert,
    task_id='load_facts_table_songplays',
    dag=dag
)

load_users_dimension_table = LoadDimensionOperator(
    redshift_conn_id='redshift',
    table='users',
    clear_previous=True,
    sql_query=SqlQueries.user_table_insert,
    task_id='load_dimension_table_users',
    dag=dag
)

load_artists_dimension_table = LoadDimensionOperator(
    redshift_conn_id='redshift',
    table='artists',
    clear_previous=True,
    sql_query=SqlQueries.artist_table_insert,
    task_id='load_dimension_table_artists',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    redshift_conn_id='redshift',
    table='time',
    clear_previous=True,
    sql_query=SqlQueries.time_table_insert,
    task_id='load_dimension_table_time',
    dag=dag
)

load_songs_dimension_table = LoadDimensionOperator(
    redshift_conn_id='redshift',
    table='songs',
    clear_previous=True,
    sql_query=SqlQueries.song_table_insert,
    task_id='load_dimension_table_song',
    dag=dag
)

run_quality_checks_facts = DataQualityOperator(
    task_id='Run_data_quality_checks_on_facts_table',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["songplays"]
)

run_quality_checks_dimension = DataQualityOperator(
    task_id='Run_data_quality_checks_on_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['users', 'artists', 'time', 'songs']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task >> [stage_songs_task, stage_events_task]

[stage_songs_task, stage_events_task] >> load_songs_play_fact_table

load_songs_play_fact_table >> run_quality_checks_facts

run_quality_checks_facts >> [load_artists_dimension_table, load_songs_dimension_table,
                             load_time_dimension_table, load_users_dimension_table]

[load_artists_dimension_table, load_songs_dimension_table,
 load_time_dimension_table, load_users_dimension_table] >> run_quality_checks_dimension

run_quality_checks_dimension >> end_operator
