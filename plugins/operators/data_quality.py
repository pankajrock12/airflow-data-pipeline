
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    @apply_defaults
    def __init__(self, redshift_conn_id, tests, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        for test in self.tests:
            result = redshift.get_records(test["check_sql"])[0][0]
            if result != test["expected_result"]:
                raise ValueError(f"Data quality check failed: {test}")
