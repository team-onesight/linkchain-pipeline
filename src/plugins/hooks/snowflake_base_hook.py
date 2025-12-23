from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector import SnowflakeConnection
from abc import ABC

class CustomSnowflakeBaseHook(ABC, SnowflakeHook):
    """
    CustomSnowflakeBaseHook은 snowflake 데이터베이스에 연결하기 위한 커스텀 베이스 훅입니다.
        
    :param snowflake_conn_id: The Airflow connection ID to use for Snowflake.
    :param database: The database to use for the session.
    :param schema: The schema to use for the session.
    """
    conn : SnowflakeConnection = None

    def __init__(self, 
                 snowflake_conn_id: str = "snowflake_conn", 
                 database: str = None, 
                 schema: str = None, 
                 *args, **kwargs):
        super().__init__(snowflake_conn_id=snowflake_conn_id, *args, **kwargs)
        self.database = database
        self.schema = schema

    def get_conn(self) -> SnowflakeConnection:
        """
        get connection
        """
        if self.conn:
            return self.conn
        else :
            conn = super().get_conn()

            if self.database or self.schema:
                with conn.cursor() as cur:
                    if self.database:
                        self.log.info(f"Setting session database to: {self.database}")
                        cur.execute(f"USE DATABASE {self.database}")
                    if self.schema:
                        self.log.info(f"Setting session schema to: {self.schema}")
                        cur.execute(f"USE SCHEMA {self.schema}")
        
        return conn
