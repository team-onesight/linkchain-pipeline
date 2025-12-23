from hooks.snowflake_base_hook import CustomSnowflakeBaseHook

class SnowflakeODSQueryHook(CustomSnowflakeBaseHook):
    """
    SnowflakeODSQueryHook은 snowflake 데이터베이스에 ODS 스키마를 로드하기 위한 커스텀 훅입니다.
    쿼리를 실행하고 결과를 받을 때 사용하시기 바랍니다.
        
    :param snowflake_conn_id: The Airflow connection ID to use for Snowflake.
    """

    def __init__(self, 
                 snowflake_conn_id: str = "snowflake_conn",
                 *args, **kwargs):
        super().__init__(snowflake_conn_id=snowflake_conn_id, 
                         database="LINKCHAIN", 
                         schema="ODS", 
                         *args, **kwargs)
