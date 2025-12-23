from plugins.hooks.snowflake_base_hook import CustomSnowflakeBaseHook

class SnowflakeCommandHook(CustomSnowflakeBaseHook):
    """
    SnowflakeCommandHook은 snowflake 데이터베이스에 command를 실행하기 위한 커스텀 훅입니다.
    INSERT, COPY INTO 등 결과를 반환하지 않거나 multi-schema를 다뤄야 하는 쿼리를 실행할 때 사용하시기 바랍니다.

    :param snowflake_conn_id: The Airflow connection ID to use for Snowflake.
    :param database: The database to use for the session.
    :param schema: The schema to use for the session.
    """

    def __init__(self, 
                 snowflake_conn_id: str = "snowflake_conn", 
                 database: str = None, 
                 schema: str = None, 
                 *args, **kwargs):
        super().__init__(snowflake_conn_id=snowflake_conn_id, 
                         database=database,
                         schema=schema,
                         *args, **kwargs)

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
