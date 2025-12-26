from airflow.models import BaseOperator
from hooks.postgres_olap_to_oltp_hook import PostgresOlapToOltpHook


class CreateMappingOperator(BaseOperator):
    """
    staging + public → mapping table 생성
    """

    def __init__(self, mapping_sql, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping_sql = mapping_sql

    def execute(self, context) -> None:
        pg_hook = PostgresOlapToOltpHook(postgres_conn_id="postgres_default")
        pg_hook.upsert_table(self.mapping_sql)
