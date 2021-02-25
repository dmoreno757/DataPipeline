from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    sqlSelect = """SELECT COUNT(*) FROM {};"""

    ui_color = '#89DA59'

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
                 dq_checks = "",
                 truncate = False,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.dq_checks = dq_checks
        self.truncate = truncate
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        '''
        Description:
        This functions does a quality check of the count of a table

        Arguments:
        self: Instance of the class
        context: Can have different values
    
        Returns:
        None
        '''
        
        self.log.info = ('DataQualityOperator startup')
        
        self.log.info = ('Set up the hook')
        redshiftHook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info = ("Go through the tables to grab a count")
        for x in self.dq_checks:
            sql = x.get('check_sql')
            output = redshiftHook.get_records(sql)[0]
        
        self.log.info = ('Condition to see if the data is correct')
        self.log.info = (output)
        if len(output) < 1:
            raise ValueError (f"data check failed for {sql}")
        
        self.log.info = ("Complete")