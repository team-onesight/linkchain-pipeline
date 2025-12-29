import logging

from hooks.abc.snowflake_base_hook import CustomSnowflakeBaseHook


class SnowflakeCommandHook(CustomSnowflakeBaseHook):
    logger = logging.getLogger(__name__)
    """
    SnowflakeCommandHook은 snowflake 데이터베이스에 command를 실행하기 위한 커스텀 훅입니다.
    INSERT, COPY INTO 등 결과를 반환하지 않거나 multi-schema를 다뤄야 하는 쿼리를 실행할 때 사용하시기 바랍니다.

    :param snowflake_conn_id: The Airflow connection ID to use for Snowflake.
    :param database: The database to use for the session.
    :param schema: The schema to use for the session.
    """  # noqa: E501

    def __init__(
        self,
        *args,
        snowflake_conn_id: str = "snowflake_default",
        database: str = None,
        schema: str = None,
        **kwargs,
    ):
        super().__init__(
            *args,
            snowflake_conn_id=snowflake_conn_id,
            database=database,
            schema=schema,
            **kwargs,
        )

    def command(self, sql: str) -> list:
        """
        SQL command를 실행합니다.

        :param sql: 실행할 쿼리
        :type sql: str
        :return: 쿼리 결과
        :rtype: list
        """
        conn = self.get_conn()
        with conn.cursor() as cur:
            self.log.info(f"Executing query: {sql}")
            cur.execute(sql)
            results = cur.fetchall()
        return results

    def command_upsert_transaction(
        self,
        create_temp_table_sql: str,
        insert_sql: str,
        merge_sql: str,
        data: list,
        temp_table_name: str,
    ):
        """
        Executes a robust UPSERT transaction using the Temporary Table strategy.

        This method guarantees atomicity by wrapping the Create-Insert-Merge workflow
        within a single database connection. It ensures that either all operations
        succeed (Commit) or none take effect (Rollback) in case of an error.

        Workflow:
            1. Create a temporary table using ``create_temp_table_sql``.
            2. Batch insert ``data`` using ``insert_sql`` (via ``executemany``).
            3. Perform Upsert/Merge using ``merge_sql``.
            4. Commit the transaction.
            5. Cleanup: Drop the temporary table and close the connection.

        :param create_temp_table_sql: DDL statement to create the temporary table.
        :param insert_sql: Parameterized INSERT statement (e.g., ``VALUES (%s, %s)``).
        :param merge_sql: MERGE statement to synchronize data from temp to target table.
        :param data: A list of tuples/rows to be inserted.
        :param temp_table_name: Name of the temporary table (used for logging and cleanup).
        :raises Exception: Propagates any database errors after performing a rollback.
        """  # noqa: E501
        conn = self.get_conn()

        try:
            with conn.cursor() as cur:
                self.log.info(f"Creating temp table: {temp_table_name}")
                cur.execute(create_temp_table_sql)

                self.log.info(f"Inserting {len(data)} rows into temp table")
                cur.executemany(insert_sql, data)

                self.log.info("Executing MERGE statement...")
                cur.execute(merge_sql)
                self.log.info(f"MERGE completed. Rows affected: {cur.rowcount}")

            conn.commit()

        except Exception as e:
            self.log.error(f"Transaction failed. Rolling back. Error: {e}")
            conn.rollback()
            raise e

        finally:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            except Exception as e:
                self.logger.error("Failed to drop temp table.", exc_info=e)
                pass

            conn.close()

    def merge_combined_sources_to_integrated_table(self):
        """
        새로 들어온 user의 url과 크롤링된 url들을 merge합니다.
        title, desc, tags, embedding DAG들이 이 테이블만 바라보며 update 합니다.
        테이블(TARGET)에 해당 link_id가 없을 때만 실행됩니다.
        """
        with self.get_conn() as conn:
            cursor = conn.cursor()

            sql = """
            MERGE INTO linkchain.analytics.integrated_table AS TARGET
            USING linkchain.raw_data.combined_sources AS SOURCE
            ON TARGET.link_id = SOURCE.link_id

            WHEN NOT MATCHED THEN
                INSERT (
                    link_id,
                    url,
                    title,
                    description,
                    link_embedding
                )
                VALUES (
                    SOURCE.link_id,
                    SOURCE.url,
                    SOURCE.title,
                    SOURCE.description,
                    SOURCE.link_embedding
                );
            """
            result = cursor.execute(sql)
            return result.fetchone()

    def get_integrated_table_where_title_desc_is_null(self):
        """
        get links from combined_sources
        :param self: Description
        """
        with self.get_conn() as conn:
            cursor = conn.cursor()

            sql = """
            SELECT
                I.LINK_ID
                , M.S3_PATH
            FROM LINKCHAIN.ANALYTICS.INTEGRATED_TABLE I
            LEFT JOIN LINKCHAIN.RAW_DATA.CRAWLED_HTML_METADATA M
            ON I.LINK_ID = M.LINK_ID
            WHERE I.TITLE IS NULL AND I.DESCRIPTION IS NULL
                AND M.S3_PATH IS NOT NULL;
            """
            result = cursor.execute(sql)
        return result.fetchall()
