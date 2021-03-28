from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 column="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column = column

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        query = f"SELECT COUNT({self.column}) FROM {self.table} WHERE {self.column} IS NULL"
        records = redshift_hook.get_records(query)
        if len(records) < 1 or len(records[0]) < 1 :
            raise ValueError(f"data quality check failed : {self.column} contains nulls.")
        self.log.info(f"Data Quality check on table {self.table} passed.")