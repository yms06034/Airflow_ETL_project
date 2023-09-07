from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    """
    Operator that loads parquet files to Amazon Redshift.
    """

    ui_color = '#358140'
    copy_query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 table,
                 drop_table,
                 s3_bucket,
                 s3_folder,
                 aws_connection_id,
                 redshift_connection_id,
                 create_query,
                 copy_options,
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.drop_table = drop_table
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.aws_connection_id = aws_connection_id
        self.redshift_connection_id = redshift_connection_id
        self.create_query = create_query
        self.copy_options = copy_options

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        self.aws_instance = AwsHook(aws_conn_id=self.aws_connection_id)
        credentials = self.aws_instance.get_credentials()

        rendered_path = self.s3_folder.format(**context)
        s3_path = "s3://{}/{}/".format(self.s3_bucket, rendered_path)

        formatted_query = S3ToRedshiftOperator.copy_query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_options
        )
        if self.drop_table:
            self.log.info('Dropping {} table if it exists...'.format(
                self.table))
            self.hook.run("DROP TABLE IF EXISTS {}".format(self.table))
            self.log.info(
                "Table {} has been successfully dropped".format(
                    self.table))
        self.log.info(
            'Creating {} table if it does not exist...'.format(self.table))
        self.hook.run(self.create_query)
        self.log.info("Removing data from {}".format(self.table))
        self.hook.run("DELETE FROM {}".format(self.table))
        self.log.info('Executing copy query...')
        self.hook.run(formatted_query)
        self.log.info("copy query execution complete...")
