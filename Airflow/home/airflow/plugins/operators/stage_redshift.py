from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

class StageToRedshiftOperator(BaseOperator):
    """
    Copies JSON data from S3 to staging tables in Redshift data warehouse
    
    Parameters:
        conn_id: Redshift connection ID
        AWS: AWS credentials ID
        table: Target staging table in Redshift to copy data into
        s3_bucket: S3 bucket where JSON data resides
        s3_key: Path in S3 bucket where JSON data files reside
        json_option: Either a JSONPaths file or 'auto', mapping JSON source data to target table
        region: AWS Region where the source data is located
    """
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as json '{}'
    """
    ui_color = '#358140'
    @apply_defaults
    def __init__(self,
                 conn_id="",
                 AWS="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_option="auto",
                 region='us-west-2',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.AWS = AWS
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_option=json_option
    def execute(self, context):
        redshift = PostgresHook(self.conn_id)
        aws_hook=AwsHook(self.AWS)
        credentials = aws_hook.get_credentials()
        self.log.info(f"Clearing data from {self.table} Redshift table")
        redshift.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_option
        )
        redshift.run(formatted_sql)