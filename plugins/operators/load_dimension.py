from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class LoadDimensionOperator(BaseOperator):
    
    sqlInsert = """INSERT INTO SELECT {} {};"""
    
    sqlTrunc = """DELETE FROM {};"""
    
    

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "redshift_conn_id",
                 table = "",
                 aws_credentials_id = "aws_credentials",
                 s3_bucket = "s3_bucket",
                 s3_key = "s3_key",
                 log_json_file = "",
                 sqlWrite = "",
                 table_cloumns = "",
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket 
        self.s3_key = s3_key 
        self.log_json_file = log_json_file
        self.sqlWrite = sqlWrite
        self.truncate = truncate
  

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshiftHook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        credentials = aws_hook.get_credentials()

        
        if self.truncate:
            
            sqlTruncRun = LoadDimensionOperator.sqlTrunc.format(self.table)
            redshiftHook.run(sqlTruncRun)
        
        
        sqlRun = LoadDimensionOperator.sqlInsert.format(self.table, self.sqlWrite, credentials.access_key, credentials.secret_key, self.log_json_file)
        redshiftHook.run(sqlRun)
        
            
        #redshiftHook.run(LoadDimensionOperator.f"INSERT INTO {self.table} {self.sqlWrite}")
