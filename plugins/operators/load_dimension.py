
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    @apply_defaults
    def __init__(self, redshift_conn_id, table, sql_query, truncate_insert=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.truncate_insert:
            redshift.run(f"DELETE FROM {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
