import os

from airflow.sdk.bases.operator import BaseOperator
from airflow.utils.context import Context
from hooks.postgres_oltp_to_olap_hook import PostgresOltpToOlapHook
from hooks.snowflake_command_hook import SnowflakeCommandHook


class PostgresToSnowflakeOperator(BaseOperator):
    """
    postgreSQL에서 Snowflake로 전체 데이터 적재 수행합니다.
    utils.olap_table_config.TABLE_CONFIG의 설정을 참고해서 full refresh를 수행합니다.
    1. PostgreSQL에서 데이터를 청크 단위로 읽어옵니다.
    2. 읽어온 데이터를 임시 Parquet 파일로 저장합니다.
    3. Snowflake의 임시 스테이지에 Parquet 파일을 업로드합니다.
    4. Snowflake 테이블에 데이터를 복사합니다.
    5. 임시 파일과 스테이지를 정리합니다.

    :param postgres_conn_id: PostgreSQL 연결을 위한 Airflow Connection ID
    :param snowflake_conn_id: Snowflake 연결을 위한 Airflow Connection ID
    :param table_key: 설정 파일(TABLE_CONFIG)에서 해당 테이블을 식별하기 위한 키값
    :param table_config: 테이블 메타데이터(source_table, snowflake_table, columns 등)를 담은 딕셔너리
    :param chunk_size: 한 번에 처리할 행(row)의 개수 (메모리 사용량 조절)
    :param snowflake_db: 데이터를 적재할 Snowflake 데이터베이스 이름
    :param snowflake_schema: 데이터를 적재할 Snowflake 스키마 이름
    """  # noqa: E501
    template_fields = [("chunk_size")]

    def __init__(
        self,
        *,
        postgres_conn_id: str,
        snowflake_conn_id: str,
        table_key: str,
        table_config: dict,
        chunk_size: int,
        snowflake_db: str,
        snowflake_schema: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.table_key = table_key
        self.table_config = table_config
        self.chunk_size = chunk_size
        self.snowflake_db = snowflake_db
        self.snowflake_schema = snowflake_schema

    def execute(self, context: Context) -> None:
        """
        Execute
        """
        pg_hook = PostgresOltpToOlapHook(postgres_conn_id=self.postgres_conn_id)
        sf_hook = SnowflakeCommandHook(
            snowflake_conn_id=self.snowflake_conn_id,
            database=self.snowflake_db,
            schema=self.snowflake_schema,
        )

        source_table = self.table_config["source_table"]
        snowflake_table_full_name = self.table_config["snowflake_table"]
        columns = self.table_config["columns"]

        self.log.info(f"--- Performing full refresh for {self.table_key} ---")

        sf_hook.command(f"TRUNCATE TABLE {snowflake_table_full_name}")

        stage_name = f"{self.snowflake_db}.{self.snowflake_schema}.temp_stage_{source_table.replace('.', '_')}"  # noqa: E501
        stage_name_with_at = f"@{stage_name}"
        sf_hook.command(
            f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT = (TYPE = 'PARQUET')"
        )  # noqa: E501

        try:
            file_paths_generator = pg_hook.bulk_dump_to_parquet_files(
                table_name=source_table,
                columns=columns,
                chunk_size=self.chunk_size,
            )

            for tmp_file_path in file_paths_generator:
                try:
                    sf_hook.command(
                        f"PUT 'file://{os.path.abspath(tmp_file_path)}' {stage_name_with_at}"  # noqa: E501
                    )

                    copy_sql = f"""
                    COPY INTO {snowflake_table_full_name}
                    FROM {stage_name_with_at}
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    """
                    result = sf_hook.command(copy_sql)
                    if result and len(result) > 0 and len(result[0]) > 4:
                        self.log.info(f"COPY result: {result[0][3]} row affected")
                    else:
                        self.log.info("COPY INTO executed successfully.")
                finally:
                    os.remove(tmp_file_path)

        finally:
            self.log.info(f"Cleaning up stage {stage_name}")
            sf_hook.command(f"DROP STAGE IF EXISTS {stage_name}")
