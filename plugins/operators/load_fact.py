from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 sql='',
                 table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info('Start loading data')

        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            postgres.run(f'TRUNCATE {self.table}')

        self.log.info(f'Load fact table {self.table}')
        postgres.run(f'INSERT INTO {self.table} {self.sql}')
