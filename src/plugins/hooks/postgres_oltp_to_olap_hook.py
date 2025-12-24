import tempfile
from contextlib import closing

import pandas as pd
from hooks.postgres_base_hook import CustomPostgresBaseHook


class PostgresOltpToOlapHook(CustomPostgresBaseHook):
    """
    OLTP(Postgres) → OLAP 적재용 데이터를 읽기 위한 Hook

    - BaseHook에서 connection 재사용 및 search_path 관리
    - 실제 select / extract 로직은 task 단에서 구현
    """  # noqa: E501

    def __init__(
        self,
        postgres_conn_id: str = "postgres_dev",
        schema: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(
            *args,
            postgres_conn_id=postgres_conn_id,
            schema=schema,
            **kwargs,
        )

    def bulk_dump_to_parquet_files(
        self, table_name: str, columns: list[str], chunk_size: int
    ):
        """
        PostgreSQL 테이블의 데이터를 청크(Chunk) 단위로 읽어와 임시 Parquet 파일로 저장하고, 해당 파일의 경로를 반환합니다.

        1. 데이터를 읽어옵니다.
        2. Pandas DataFrame으로 변환 후, 시간형(Datetime) 데이터는 호환 가능한 문자열 포맷으로 변환합니다.
        3. 변환된 데이터를 로컬 임시 디렉토리에 .parquet 파일로 저장합니다.
        4. 제너레이터(Generator)를 통해 저장된 파일의 절대 경로를 순차적으로 yield 합니다.

        즉, 외부에서 요청받을 때마다 while문 1회 루프를 수행한 뒤 결과( temp file의 이름 )를 반환하도록 해서
        메모리 효율성을 높이고자 합니다.

        :param table_name: Name of the table to dump.
        :param columns: List of columns to select.
        :param chunk_size: The number of rows to fetch at a time.
        :return: A generator that yields paths to temporary Parquet files.
        """  # noqa: E501
        with closing(self.get_conn()) as conn:
            with conn.cursor(
                name=f"dump_cursor_{table_name.replace('.', '_')}"
            ) as cursor:
                cursor.itersize = chunk_size
                select_sql = f"SELECT {', '.join(columns)} FROM {table_name};"  # noqa: S608
                self.log.info(f"Executing: {select_sql}")
                cursor.execute(select_sql)

                while True:
                    records = cursor.fetchmany(chunk_size)
                    if not records:
                        break

                    df = pd.DataFrame(records, columns=columns)

                    for col_name, dtype in df.dtypes.items():
                        if pd.api.types.is_datetime64_any_dtype(dtype):
                            df[col_name] = df[col_name].dt.strftime(
                                "%Y-%m-%d %H:%M:%S.%f"
                            )

                    tmp_file = tempfile.NamedTemporaryFile(
                        mode="w+b", delete=False, suffix=".parquet"
                    )
                    df.to_parquet(tmp_file.name, index=False, engine="pyarrow")
                    tmp_file.close()
                    yield tmp_file.name
