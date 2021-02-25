from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sqlWrite = " COPY {} \
    FROM '{}' \
    ACCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    FORMAT AS json '{}';"
    

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id = "redshift_conn_id",
                 table = "",
                 aws_credentials_id = "aws_credentials",
                 s3_bucket = "s3_bucket",
                 s3_key = "s3_key",
                 log_json_file = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        

    def execute(self, context):
        '''
        Description:
        This functions process the information from s3 and stages it into reshift

        Arguments:
        self: Instance of the class
        context: Can have different values
    
        Returns:
        None
        '''
        
        self.log.info = ('Implement configuration')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info = ('Gather s3 information')

        self.s3_key = self.s3_key.format(**context)
        dataPath = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        self.log.info = ('Set up the hook')
        redshiftHook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
      
        self.log.info = ('start of the redhsift operator')
        sqlRun = StageToRedshiftOperator.sqlWrite.format(self.table, dataPath, credentials.access_key, credentials.secret_key, self.log_json_file)
        redshiftHook.run(sqlRun)
        
        self.log.info = ("End of redhsift operator")
        





