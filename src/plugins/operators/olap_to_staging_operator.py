from airflow.models import BaseOperator
from hooks.postgres_olap_to_oltp_hook import PostgresOlapToOltpHook
from hooks.snowflake_analytics_hook import SnowflakeAnalyticsQueryHook


class OlapToStagingOperator(BaseOperator):
    """
    OLAP table → OLTP staging table 데이터 적재 Operator
    """

    def __init__(
        self,
        olap_table: str,
        staging_table: str,
        staging_columns: list[str],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.olap_table = olap_table
        self.staging_table = staging_table
        self.staging_columns = staging_columns

    def execute(self, context) -> None:
        sf_hook = SnowflakeAnalyticsQueryHook()

        columns, rows = sf_hook.get_olap_table_data(
            table_name=self.olap_table,
            columns=self.staging_columns,
        )

        self.log.info(f"{len(rows)} rows fetched from {self.olap_table}")

        pg_hook = PostgresOlapToOltpHook(postgres_conn_id="postgres_conn_id")

        pg_hook.truncate_and_insert_rows(
            table=self.staging_table,
            columns=columns,
            rows=rows,
        )
