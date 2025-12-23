from hooks.snowflake_base_hook import CustomSnowflakeBaseHook


class SnowflakeAnalyticsQueryHook(CustomSnowflakeBaseHook):
    """
    SnowflakeAnalyticsQueryHook은 snowflake 데이터베이스에 Analytics 스키마를 로드하기 위한 커스텀 훅입니다.
    SELECT 쿼리 등을 실행하고 결과를 받을 때 사용하시기 바랍니다.

    :param snowflake_conn_id: The Airflow connection ID to use for Snowflake.
    """  # noqa: E501

    def __init__(self, *args, snowflake_conn_id: str = "snowflake_conn", **kwargs):
        super().__init__(
            *args,
            snowflake_conn_id=snowflake_conn_id,
            database="LINKCHAIN",
            schema="ANALYTICS",
            **kwargs,
        )
