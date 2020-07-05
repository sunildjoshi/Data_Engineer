from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads dimension table in Redshift from data in staging table(s)
    
    Parameters:
        conn_id: Redshift connection ID
        table: Target table in Redshift to load
        insert_sql: SQL query for getting data to load into target table
    """   
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 table="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.insert_sql= insert_sql
        self.table=table
       
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f"Inserting records to  {self.table} table")
        redshift.run(f"INSERT INTO {self.table} {self.insert_sql}")
