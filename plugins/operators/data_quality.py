from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id="",
        tests=[],  # List of sql tests
        *args,
        **kwargs,
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_idd = postgres_conn_id
        self.tests = tests

    def execute(self, context):
        postgres = PostgresHook(self.postgres_conn_idd)
        self.log.info("Postres connection created for the redshift dw")
        self.log.info(f"Starting validation of tests")
        for test in self.tests:
            self.log.info(f"Running test {test}")
            expected = test["expected"]
            type = test.get("type", "general")
            sql = test["sql"]
            self.log.info(f'Get records for query: "{sql}"')
            records = postgres.get_records(sql)
            num_records = records[0][0]

            if type == "count" and num_records < expected:
                raise ValueError(
                    f"Data quality check failed.\
                    The sql query {test.sql_query} returned\
                        the following results {records}"
                )

            elif num_records >= expected:
                raise ValueError(
                    f"Data quality check failed.\
                     The sql query {test.sql_query} returned\
                          the following results {records}"
                )
        self.log.info(f"Successful validation")
