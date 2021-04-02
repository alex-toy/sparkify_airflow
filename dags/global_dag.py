from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import configparser
from settings import config_file

config = configparser.ConfigParser()
config.read_file(open(config_file))

S3_BUCKET_LOG_DATA = config.get('S3','LOG_DATA')
S3_BUCKET_SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'check_null_sql' : """SELECT COUNT(*) FROM {} WHERE {} IS NULL;""",
    'check_count_sql' : """SELECT COUNT(*) FROM {};"""
}

# DAG
dag = DAG(
    'global_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    S3_bucket="udacity-dend",
    S3_key="log_data",
    delimiter=",",
    formatting=f"FORMAT AS json '{LOG_JSONPATH}'"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    S3_bucket="udacity-dend",
    S3_key="song_data",
    delimiter=",",
    formatting="JSON 'auto'"
)



load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    query=SqlQueries.user_table_insert,
    append=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    query=SqlQueries.song_table_insert,
    append=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    query=SqlQueries.artist_table_insert,
    append=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    query=SqlQueries.time_table_insert,
    append=False
)

run_quality_checks = DataQualityOperator(
    task_id='run_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_null_sql=default_args['check_null_sql'],
    check_count_sql=default_args['check_count_sql'],
    tables=['songplays', 'songs', 'users', 'artists', 'time'],
    columns=['playid', 'songid', 'userid', 'artistid', 'start_time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables_task >> \
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> \
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
run_quality_checks >> end_operator

