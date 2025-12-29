from airflow.sdk.bases.operator import BaseOperator
from hooks.postgres_olap_to_oltp_hook import PostgresOlapToOltpHook


class StagingToTargetOperator(BaseOperator):
    """
    staging.{name} → public.{name} 테이블로 upsert하는 Operator
    """

    def __init__(self, upsert_sql, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.upsert_sql = upsert_sql

    def execute(self, context) -> None:
        pg_hook = PostgresOlapToOltpHook(postgres_conn_id="postgres_default")
        pg_hook.upsert_table(self.upsert_sql)
