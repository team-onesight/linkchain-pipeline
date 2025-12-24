from hooks.snowflake_base_hook import CustomSnowflakeBaseHook


class SnowflakeRawDataQueryHook(CustomSnowflakeBaseHook):
    """
    SnowflakeRawDataQueryHook은 snowflake 데이터베이스에 raw data 스키마를 로드하기 위한 커스텀 훅입니다.
    쿼리를 실행하고 결과를 받을 때 사용하시기 바랍니다.

    :param snowflake_conn_id: The Airflow connection ID to use for Snowflake.
    """  # noqa: E501

    def __init__(self, *args, snowflake_conn_id: str = "snowflake_conn", **kwargs):
        super().__init__(
            *args,
            snowflake_conn_id=snowflake_conn_id,
            database="LINKCHAIN",
            schema="RAW_DATA",
            **kwargs,
        )

    def get_links_need_to_be_fetched(self, limit: int = 100):
        """
        get Links need to fetch HTML
        :param limit: 최대 가져올 링크 수
        :type limit: int
        """
        with self.get_conn() as conn:
            cursor = conn.cursor()

            sql = f"""
            SELECT
                link_id
                ,url
                ,created_at
            FROM LINKCHAIN.RAW_DATA.LINK_NEED_TO_BE_FETCHED
            LIMIT {limit}
            """  # noqa: S608
            result = cursor.execute(sql)
            return result.fetchall()
