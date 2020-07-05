from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension table in Redshift from data in staging table(s)
    
    Parameters:
        conn_id: Redshift connection ID
        table: Target table in Redshift to load
        insert_sql: SQL query for getting data to load into target table
        truncate: Whether truncate (True) or append(False/None) to table
        primary_key: When  the appending to table the column to check if the row already exists in the target table. 
                     If there is a match the row in the target table will then be updated
    """
    
    ui_color = '#80BD9E'
        
    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 insert_sql="",
                 primary_key=None,
                 table="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.insert_sql= insert_sql
        self.table=table
        self.truncate=truncate
        self.primary_key=primary_key
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        if self.truncate:
            self.log.info(f"Clearing data from {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        elif self.primary_key:
            self.log.info(f"Removing matching records from {self.table} to be updated")        
            redshift.run(f'''DELETE FROM {self.table} 
                             WHERE {self.primary_key} IN (
                             SELECT {self.primary_key} 
                             FROM ({self.insert_sql}))''')
        self.log.info(f"Inserting records to  {self.table} table")
        redshift.run(f"INSERT INTO {self.table} {self.insert_sql}")