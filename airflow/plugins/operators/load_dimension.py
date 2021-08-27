from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from typing import Any
from datetime import datetime,timedelta
from airflow.utils.task_group import TaskGroup

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    def __init__(self,redshift_conn_id:str,table_name:str, sql_statement:str, delete_load:False, *args ,**kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement= sql_statement
        self.table_name = table_name
        self.delete_load = delete_load
        

    def execute(self, context: Any):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_load:
            self.log.info(f"Delete load operation set to TRUE. Running delete statement on table {self.table_name}")
            redshift_hook.run(f"DELETE FROM {self.table_name}")
        self.log.info(f"Running query to load data into Dimension Table {self.table_name}")
        redshift_hook.run(self.sql_statement)
        self.log.info(f"Dimension Table {self.table_name} loaded")