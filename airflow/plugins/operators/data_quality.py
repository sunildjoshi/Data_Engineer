from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by passing test SQL query and expected result 
    
    Parameters
        conn_id: Redshift connection ID 
        query: SQL query to run on Redshift
        test_value: Expected result test_query 
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 query="",
                 test_value="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query=query
        self.test_value=test_value
    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info("Running Quality Check")
        records=redshift.get_records(self.query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality failed, got no results")
            self.log.error(f"Data quality failed, got no results")
        else:
            num_records = records[0][0]
            if num_records==self.test_value:
                raise ValueError(f"Data quality failed expected {test_value} got {num_records}") 
                self.log.error(f"Data quality failed expected {value} got {num_records}")
            else:
                self.log.info(f"Data quality passed")