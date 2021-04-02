from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 null_check={},
                 table=[],
                 columns=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.null_check = null_check
        self.tables = tables
        self.columns = columns

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table, column in zip(self.tables, self.columns) :
            check_query = self.null_check['check_sql'].format(self.table, self.column)
            exp_result = self.null_check['expected_result']
            records = redshift.get_records(check_query)[0]
            error_count = 0
            failing_tests = []
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(check_query)

        if error_count > 0:
            self.log.info('SQL Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')

        if error_count == 0:
            self.log.info('SQL Tests Passed')
            
            

        
        
        
        