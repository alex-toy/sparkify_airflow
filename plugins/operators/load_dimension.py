from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.append == False:
            sql_statement = truncate_sql.format(self.table) + self.query
        else:
            sql_statement = self.query
        
        redshift.run(sql_statement)
        self.log.info(f"LoadDimensionOperator : {self.query}")

        
        
        
        