from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table = "",
                 aws_credentials_id = "",
                 s3_bucket = "",
                 s3_key = "",
                 log_json_file = "",
                 sqlWrite = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.sqlWrite = sqlWrite

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        
        redshiftHook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshiftHook.run(self.sqlWrite)
        
