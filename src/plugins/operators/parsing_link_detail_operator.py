from urllib.parse import urlparse

import pandas as pd
from airflow.sdk.bases.operator import BaseOperator
from hooks.snowflake_command_hook import SnowflakeCommandHook
from hooks.snowflake_ods_hook import SnowflakeODSQueryHook


class ParseLinkDetailOperator(BaseOperator):
    """
    Snowflake의 ODS 테이블로부터 URL 데이터를 읽어 상세 정보(Subdomain, Host 등)를 파싱하고,
    그 결과를 Snowflake의 목적 테이블에 적재합니다.
    데이터를 청크(Chunk) 단위로 처리하며,
    각 청크에 대해 파이썬의 'urllib.parse' 라이브러리를 사용해 URL 구조를 분해합니다.

    1. SnowflakeODSQueryHook을 사용하여 소스 테이블에서 데이터를 청크 단위로 가져옵니다.
    2. 목적 테이블(dest_table)을 TRUNCATE 하여 Full Refresh를 준비합니다.
    3. 각 청크별로 URL을 파싱하여 상세 필드(Subdomain, Host, Path 등)를 추출합니다.
    4. 파싱된 데이터를 Pandas DataFrame으로 재구성합니다.
    5. SnowflakeCommandHook을 통해 Bulk Insert를 수행합니다.

    :param snowflake_db: 작업을 수행할 Snowflake 데이터베이스 이름
    :param source_table: URL 데이터가 포함된 소스 테이블 이름 (e.g., 'ods.link')
    :param source_columns: 소스 테이블에서 조회할 컬럼 리스트 (Variable에서 가져옵니다.)
    :param dest_table: 파싱 결과가 적재될 목적 테이블 이름 (e.g., 'raw_data.link_detail')
    :param dest_columns: 목적 테이블의 전체 컬럼 리스트
    :param chunk_size: 한 번에 처리할 행(row)의 개수
    """  # noqa: E501

    def __init__(self,
                 *args,
                 snowflake_db : str,
                 snowflake_conn_id: str,
                 source_table: str,
                 source_columns: list[str],
                 dest_table: str,
                 dest_columns: list[str],
                 chunk_size: int,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.snowflake_db = snowflake_db
        self.snowflake_conn_id = snowflake_conn_id
        self.source_table = source_table
        self.source_columns = source_columns
        self.dest_table = dest_table
        self.dest_columns = dest_columns
        self.chunk_size = chunk_size

    def execute(self, context):
        """
        Execute
        """
        ods_hook = SnowflakeODSQueryHook(
            snowflake_conn_id=self.snowflake_conn_id
        )

        # 1
        link_chunks_generator = ods_hook.get_links(
            self.source_table, self.source_columns, self.chunk_size
        )

        dest_schema, dest_table_name = self.dest_table.split('.')
        cmd_hook = SnowflakeCommandHook(
            snowflake_conn_id=self.snowflake_conn_id,
            database=self.snowflake_db, schema=dest_schema
        )

        # 2
        cmd_hook.command(f"TRUNCATE TABLE {dest_table_name}")

        total_rows_loaded = 0
        for df_chunk in link_chunks_generator:
            self.log.info("Processing a chunk...")

            self.log.info("Parsing URLs for the chunk...")

            # 3
            parsed_data = df_chunk['URL'].apply(self.parse_url_details)
            df_parsed = pd.DataFrame(
                parsed_data.tolist(),
                index=df_chunk.index,
                columns=self.dest_columns[1:]
                )

            # 4
            df_final_chunk = pd.concat([df_chunk['LINK_ID'], df_parsed], axis=1)
            df_final_chunk.columns = self.dest_columns

            self.log.info(f"Loading chunk into {self.dest_table} using bulk insert...")

            records = df_final_chunk.to_records(index=False).tolist()

            # 5
            conn = cmd_hook.get_conn()
            try:
                with conn.cursor() as cur:
                    self.log.info(f"Bulk inserting {len(records)} rows into {dest_table_name}...")  # noqa: E501
                    insert_sql = f"""
                    INSERT INTO {dest_table_name} (
                        {', '.join(self.dest_columns)}
                    ) VALUES (
                        {', '.join(['%s'] * len(self.dest_columns))}
                    )
                    """  # noqa: S608
                    cur.executemany(insert_sql, records)

                conn.commit()
                self.log.info("Transaction committed successfully.")

            except Exception as e:
                self.log.error(f"Transaction failed. Rolling back. Error: {e}")
                conn.rollback()
                raise e
            finally:
                conn.close()

            total_rows_loaded += len(records)
            self.log.info(f"Loaded a chunk of {len(records)} rows.")

        self.log.info("Successfully loaded a total of"
                      + f"{total_rows_loaded} rows into {self.dest_table}.")

    def parse_url_details(self, url):
        """
        개별 URL 문자열을 파싱하여 구성 요소별로 분리합니다.

        - Subdomain: 호스트네임의 서브 도메인 (존재할 때)
        - Host: 메인 도메인 정보
        - Path: URL 경로 ( / 이후 문자열 )
        - Parameters: 쿼리 스트링 (? 이후 문자열)
        - Fragment: 해시 앵커 (# 이후 문자열)

        :param url: 파싱할 원본 URL 문자열
        :return: (subdomain, host, path, parameters, fragment) tuple
        """
        try:
            parsed_url = urlparse(url)
            hostname = parsed_url.hostname
            subdomain = None
            host = None
            if hostname:
                parts = hostname.split('.')
                if len(parts) > 2:
                    subdomain = parts[0]
                    host = '.'.join(parts[1:])
                else:
                    host = hostname

            path = parsed_url.path if parsed_url.path else None
            parameters = parsed_url.query if parsed_url.query else None
            fragment = parsed_url.fragment if parsed_url.fragment else None

            return subdomain, host, path, parameters, fragment
        except Exception:
            return None, None, None, None, None

