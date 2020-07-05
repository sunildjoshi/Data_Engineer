from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from operators import (StageToRedshiftOperator,LoadFactOperator,LoadDimensionOperator, DataQualityOperator)
from airflow.models import Variable
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False   
}

dag = DAG('udac_example_dag',
          default_args=default_args,      
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables=PostgresOperator(task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql")

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id = "redshift",
    AWS="aws_credentials",
    table="staging_events",
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    json_option='s3://udacity-dend/log_json_path.json'
    )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id = 'redshift',
    AWS='aws_credentials',
    table='staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id='redshift',
    table="songplays",
    insert_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    conn_id='redshift',
    insert_sql=SqlQueries.user_table_insert,
    truncate=False,
    primary_key="userid"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id='redshift',
    insert_sql=SqlQueries.time_table_insert,
    truncate=False,
    primary_key=None,
    table="time"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id='redshift',
    insert_sql=SqlQueries.song_table_insert,
    truncate=True,
    primary_key=None,
    table='songs'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    conn_id='redshift',
    dag=dag,
    insert_sql=SqlQueries.artist_table_insert,
    truncate=True,
    primary_key="",
    table="artists"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    query='SELECT count(*) FROM users WHERE userid is NULL',
    test_value=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_time_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
run_quality_checks >> end_operator