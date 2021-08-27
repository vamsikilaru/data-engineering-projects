from datetime import datetime,timedelta
from airflow import DAG
from helpers.sql_queries import SQLQueries
from airflow.operators.dummy import DummyOperator
from operators.load_dimension import LoadDimensionOperator
from airflow.utils.dates import days_ago 

def load_dimension_subdag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    sql_statement,
    delete_load,
    table_name,
    *args,
    **kwargs
):
    dag = DAG(f"{parent_dag_name}.{task_id}",**kwargs)
    load_dimension_table = LoadDimensionOperator(
        task_id=task_id,
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql_statement=sql_statement,
        delete_load=delete_load,
        table_name=table_name,
        start_date=days_ago(1)
    )
    load_dimension_table
    return dag

