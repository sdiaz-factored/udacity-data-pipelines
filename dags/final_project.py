import os
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from udacity.common.final_project_sql_statements import (
    DataQualityTest,
    SqlQueries,
)

default_args = {
    "owner": "udacity",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "catchup": False,
    "start_date": datetime(2018, 11, 1),
}

REDSHIFT_CONN_ID = "sdiaz_redshift"
AWS_CREDENTIALS_ID = "sdiaz_aws"
S3_BUCKET = "sfiaz-airflow"
S3_LOG_KEY = "log_data/{execution_date.year}/{execution_date.month}"
S3_SONG_KEY = "song_data"


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
)
def final_project():
    start_operator = DummyOperator(task_id="Begin_execution")

    end_operator = DummyOperator(task_id="End_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        table="staging_events",
        s3_bucket=S3_BUCKET,
        s3_key=S3_LOG_KEY,  ## TEMPLATED PARAMETER
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        table="staging_songs",
        s3_bucket=S3_BUCKET,
        s3_key=S3_LOG_KEY,
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.songplay_table_insert,
        table="songplays",
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.user_table_insert,
        table="users",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.song_table_insert,
        table="songs",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.artist_table_insert,
        table="artists",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.time_table_insert,
        table="time",
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        postgres_conn_id=REDSHIFT_CONN_ID,
        tests=DataQualityTest.tests,
    )

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()
