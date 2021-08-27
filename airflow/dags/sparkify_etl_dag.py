from os import getenv

from airflow.utils.dates import days_ago
from airflow import DAG 
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
#from airflow.operators.udacity_plugin import LoadDimensionOperator
#from udacity_plugin.helpers import SQLQueries
from sparkify_dimension_subdag import load_dimension_subdag
from airflow.operators.subdag import SubDagOperator
from datetime import datetime,timedelta
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from helpers.sql_queries import SQLQueries

S3_BUCKET = getenv("S3_BUCKET","udacity0821")
S3_INPUT_KEY= getenv("S3_INPUT_KEY","data/song_data")
REDSHIFT_TABLE = getenv("REDSHIGT_TABLE","staging_songs")
JSON_PATH = getenv("JSON_PATH","log_json_path.json")

dag_name = 'udacity_warehouse_dag'
with DAG (
    dag_name, 
    start_date=days_ago(1), 
    schedule_interval= None, 
    tags=['example']
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',dag=dag)

    task_trasfer_s3_to_redshift = S3ToRedshiftOperator(
        schema="public",
        table=REDSHIFT_TABLE,
        s3_bucket=S3_BUCKET,
        s3_key=S3_INPUT_KEY,
        aws_conn_id='aws_credentials',
        verify= False,
        copy_options= ["FORMAT AS json 'auto'"],
        redshift_conn_id = 'redshift',
        truncate_table = True,
        task_id = 'transfer_s3_to_redshift'
    )
    task_trasfer_s3_to_redshift_log_events = S3ToRedshiftOperator(
        schema="public",
        table="staging_events",
        s3_bucket=S3_BUCKET,
        s3_key="data/log_data",
        aws_conn_id='aws_credentials',
        verify= False,
        copy_options= [f"FORMAT AS json 's3://{S3_BUCKET}/log_json_path.json'"],
        redshift_conn_id = 'redshift',
        truncate_table = True,
        task_id = 'transfer_s3_to_redshift_log_events'
    )

    task_load_songplays_fact_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id='redshift',
        sql_statement=SQLQueries.songplay_table_insert,
        delete_load= True,
        table_name='songplays',
        dag=dag
    )

    task_load_user_dimension_table = SubDagOperator(
        subdag = load_dimension_subdag(
            parent_dag_name=dag_name,
            task_id="Load_user_dim_table",
            redshift_conn_id='redshift',
            sql_statement=SQLQueries.user_table_insert,
            delete_load= True,
            table_name='users'
        ),
        task_id="Load_user_dim_table",
        dag=dag
    )

    task_load_song_dimension_table = SubDagOperator(
        subdag = load_dimension_subdag(
            parent_dag_name=dag_name,
            task_id="Load_song_dim_table",
            redshift_conn_id='redshift',
            sql_statement=SQLQueries.song_table_insert,
            delete_load= True,
            table_name='songs'
        ),
        task_id="Load_song_dim_table",
        dag=dag
    )
    task_load_artist_dimension_table = SubDagOperator(
        subdag = load_dimension_subdag(
            parent_dag_name=dag_name,
            task_id="Load_artist_dim_table",
            redshift_conn_id='redshift',
            sql_statement=SQLQueries.artist_table_insert,
            delete_load= True,
            table_name='users'
        ),
        task_id="Load_artist_dim_table",
        dag=dag
    )

    task_load_time_dimension_table = LoadDimensionOperator(
            task_id="Load_time_dim_table",
            redshift_conn_id='redshift',
            sql_statement=SQLQueries.time_table_insert,
            delete_load= True,
            table_name='time',
            dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution',dag=dag)

    start_operator >> [ task_trasfer_s3_to_redshift,task_trasfer_s3_to_redshift_log_events] >> task_load_songplays_fact_table
    task_load_songplays_fact_table  >> task_load_time_dimension_table
    task_load_time_dimension_table >> [ task_load_user_dimension_table, task_load_song_dimension_table, task_load_artist_dimension_table]  >> end_operator
