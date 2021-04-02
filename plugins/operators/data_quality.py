from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_null_sql="",
                 check_count_sql="",
                 tables=[],
                 columns=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_null_sql = check_null_sql
        self.check_count_sql = check_count_sql
        self.tables = tables
        self.columns = columns

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table, column in zip(self.tables, self.columns) :
            check_query = self.check_null_sql.format(table, column)
            records = redshift.get_records(check_query)[0]
            error_count = 0
            failing_tests = []
            if records[0] != 0 :
                error_count += 1
                failing_tests.append(check_query)

        if error_count > 0:
            self.log.info('SQL Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        else :
            self.log.info('SQL Tests Passed')
            
        
        for table in self.tables:
            query = self.check_count_sql.format(table)
            records = redshift.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Passed data quality record count on table {table} check with {records[0][0]} records")
            
            

        
        
        
        