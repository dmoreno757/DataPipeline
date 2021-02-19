from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries



# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')




default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 1, 4),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

task_create_tables = PostgresOperator(
    task_id = 'create_tables',
    dag = dag,
    sql = 'create_tables.sql',
    postgres_conn_id = 'redshift_conn_id'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Events',
    dag=dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift_conn_id',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    region = 'us_west_2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Songs',
    dag=dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift_conn_id',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    region = 'us_west_2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = 'songplays',
    redshift_conn_id = 'redshift_conn_id',
    aws_credentials_id = 'aws_credentials',
    sqlWrite = SqlQueries.songplay_table_insert,
    provide_context = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    redshift_conn_id = 'redshift_conn_id',
    aws_credentials_id = 'aws_credentials',
    sqlWrite = SqlQueries.user_table_insert   
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'songs',
    redshift_conn_id = 'redshift_conn_id',
    aws_credentials_id = 'aws_credentials',
    sqlWrite = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = 'artists',
    redshift_conn_id = 'redshift_conn_id',
    aws_credentials_id = 'aws_credentials',
    sqlWrite = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    destination_table = 'time',
    redshift_conn_id = 'redshift_conn_id',
    aws_credentials_id = 'aws_credentials',
    sqlWrite = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> task_create_tables

task_create_tables >> stage_events_to_redshift
task_create_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table

stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
