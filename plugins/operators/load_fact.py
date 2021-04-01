from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    table_insert = """
        INSERT INTO {} {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        sql_statement = LoadFactOperator.table_insert.format(self.table, self.query)
        redshift.run(sql_statement)
        self.log.info(f"LoadFactOperator : {self.query}")

        

