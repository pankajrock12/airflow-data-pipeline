
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2023,1,1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False)

start = DummyOperator(task_id='Begin_execution', dag=dag)
end = DummyOperator(task_id='End_execution', dag=dag)

stage_events = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    s3_bucket='your-bucket',
    s3_key='log-data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials'
)

stage_songs = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    s3_bucket='your-bucket',
    s3_key='song-data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials'
)

load_songplays = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    sql_query='SELECT * FROM staging_events'
)

quality = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tests=[{"check_sql": "SELECT COUNT(*) FROM songplays", "expected_result": 1}]
)

start >> [stage_events, stage_songs] >> load_songplays >> quality >> end
