
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    @apply_defaults
    def __init__(self, redshift_conn_id, table, sql_query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
