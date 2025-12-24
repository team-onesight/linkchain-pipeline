import uuid
from typing import Any, Sequence

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from common.hash_utils import get_uuid_hash
from common.iterable_utils import flat_map
from hooks.snowflake_command_hook import SnowflakeCommandHook


class SnowflakeUpsertLinksOperator(BaseOperator):
    """
    XCom에서 수집된 링크 목록을 가져와 Snowflake에 Merge(Upsert)하는 오퍼레이터
    target_table: LINKCHAIN.RAW_DATA.URL_CRAWLED
    """

    template_fields: Sequence[str] = (
        "source_task_id",
        "database",
        "schema",
        "target_table",
    )

    def __init__(
        self,
        *,
        source_task_id: str,
        database: str = "LINKCHAIN",
        schema: str = "RAW_DATA",
        target_table: str = "URL_CRAWLED",
        conn_id: str = "snowflake_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.database = database
        self.schema = schema
        self.target_table = target_table
        self.conn_id = conn_id

    def execute(self, context: Any) -> None:
        ti = context["ti"]

        raw_data = ti.xcom_pull(task_ids=self.source_task_id, key="return_value")
        if not raw_data:
            raise AirflowSkipException(
                f"No data found from task: {self.source_task_id}"
            )

        urls = set(flat_map(None, raw_data))
        if not urls:
            raise AirflowSkipException("No valid URLs to insert.")

        data_to_insert = [
            (get_uuid_hash(url), url) for url in urls if url and isinstance(url, str)
        ]

        full_target_table = f"{self.database}.{self.schema}.{self.target_table}"
        safe_task_id = self.source_task_id.replace(".", "_")
        temp_table = f"{self.database}.{self.schema}.link_temp_{safe_task_id}_{uuid.uuid4().hex[:8]}"  # noqa: E501

        create_sql = f"""
            CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table} (
                link_id     VARCHAR,
                url         VARCHAR(2048),
                created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
        """

        insert_sql = f"INSERT INTO {temp_table} (link_id, url) VALUES (%s, %s)"  # noqa: S608

        merge_sql = f"""
            MERGE INTO {full_target_table} AS target
            USING {temp_table} AS source
            ON target.link_id = source.link_id
            WHEN MATCHED THEN
                UPDATE SET target.updated_at = source.created_at
            WHEN NOT MATCHED THEN
                INSERT (link_id, url, created_at)
                VALUES (source.link_id, source.url, source.created_at)
        """  # noqa: S608

        self.log.info(f"Upserting {len(data_to_insert)} links to {full_target_table}")

        hook = SnowflakeCommandHook(snowflake_conn_id=self.conn_id)
        hook.command_upsert_transaction(
            create_temp_table_sql=create_sql,
            insert_sql=insert_sql,
            merge_sql=merge_sql,
            data=data_to_insert,
            temp_table_name=temp_table,
        )

        self.log.info("Snowflake merge transaction completed successfully.")
