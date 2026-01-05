from contextlib import closing

from hooks.abc.snowflake_base_hook import CustomSnowflakeBaseHook


class SnowflakeAnalyticsQueryHook(CustomSnowflakeBaseHook):
    """
    SnowflakeAnalyticsQueryHook은 snowflake 데이터베이스에 Analytics 스키마를 로드하기 위한 커스텀 훅입니다.
    SELECT 쿼리 등을 실행하고 결과를 받을 때 사용하시기 바랍니다.

    :param snowflake_conn_id: The Airflow connection ID to use for Snowflake.
    """  # noqa: E501

    def __init__(self, *args, snowflake_conn_id: str = "snowflake_default", **kwargs):
        super().__init__(
            *args,
            snowflake_conn_id=snowflake_conn_id,
            database="LINKCHAIN",
            schema="ANALYTICS",
            **kwargs,
        )

    def get_olap_table_data(self, table_name: str, columns: list[str]):
        """
        지정된 OLAP 테이블에서 모든 데이터를 가져옵니다.
        """

        column_odrer = ", ".join(columns)
        sql = f"SELECT {column_odrer} FROM {table_name}" # noqa: S608

        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                self.log.info(f"Fetching OLAP data from {table_name}")

                cursor.execute(sql)
                rows = cursor.fetchall()

        return columns, rows

    def get_urls_without_tags(self):
        """
        Tag가 없는 URL 목록을 가져옵니다
        """
        with self.get_conn() as conn:
            cursor = conn.cursor()

            sql = """
            SELECT
                LINK_ID,
                TITLE,
                DESCRIPTION
            FROM LINKCHAIN.ANALYTICS.INTEGRATED_TABLE
            WHERE
                LINK_ID NOT IN (
                    SELECT LINK_ID
                    FROM LINKCHAIN.ANALYTICS.TAG
                )
                AND TITLE IS NOT NULL
                AND DESCRIPTION IS NOT NULL;
            """
            result = cursor.execute(sql)
        return result.fetchall()

    def get_urls_without_embeddings(self):
        """
        임베딩이 없는 통합 테이블을 가져옵니다.
        """
        with self.get_conn() as conn:
            cursor = conn.cursor()

            sql = """
            SELECT
                I.LINK_ID,
                I.TITLE,
                I.DESCRIPTION,
                ARRAY_AGG(T.TAG_NAME) AS TAGS
            FROM LINKCHAIN.ANALYTICS.INTEGRATED_TABLE I
            INNER JOIN LINKCHAIN.ANALYTICS.TAG T
                ON I.LINK_ID = T.LINK_ID
            WHERE I.LINK_EMBEDDING IS NULL
            GROUP BY I.LINK_ID, I.TITLE, I.DESCRIPTION
            """
            result = cursor.execute(sql)
        return result.fetchall()
