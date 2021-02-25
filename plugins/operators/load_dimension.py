from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class LoadDimensionOperator(BaseOperator):
    
    sqlInsert = """
         INSERT INTO {} {};
    """
    
    sqlTrunc = """
        TRUNCATE TABLE {};
    """
    
    

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
        
        '''
        Description:
        This functions process the the dimension tables. The target table is emptied first before it needs to be inserted.

        Arguments:
        self: Instance of the class
        context: Can have different values
    
        Returns:
        None
        '''
        self.log.info = ('LoadDimensionOperator startup')
        
        self.log.info = ('Set up the hook to be used')
        redshiftHook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info = ('Truncate the table')
        if self.truncate:
            sqlTruncRun = LoadDimensionOperator.sqlTrunc.format(self.table)
            redshiftHook.run(sqlTruncRun)
        
        self.log.info = ('Start of LoadDimension ')
        sqlRun = LoadDimensionOperator.sqlInsert.format(self.table, self.sqlWrite)
        redshiftHook.run(sqlRun)
        
        self.log.info = ('End of LoadDimension ')

  