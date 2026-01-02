from abc import ABC

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection as PGConnection


class CustomPostgresBaseHook(ABC, PostgresHook):
    """
    CustomPostgresBaseHook은 postgres 데이터베이스에 연결하기 위한 커스텀 베이스 훅입니다.

    :param postgres_conn_id: airflow에서 설정된 postgres connection id
    :param schema: schema to set for the session
    """  # noqa: E501

    conn: PGConnection = None

    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        schema: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, postgres_conn_id=postgres_conn_id, **kwargs)
        self.schema = schema

    def get_conn(self) -> PGConnection:
        """
        Get or create postgres connection
        """
        if self.conn:
            return self.conn

        conn = super().get_conn()
        conn.autocommit = False

        if self.schema:
            with conn.cursor() as cur:
                self.log.info(f"Setting search_path: {self.schema}")
                cur.execute(f"SET search_path TO {self.schema}")

        self.conn = conn
        return conn
