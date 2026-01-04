from contextlib import closing
from typing import Any, Iterable

from hooks.abc.postgres_base_hook import CustomPostgresBaseHook


class PostgresTransactionalHook(CustomPostgresBaseHook):
    """
    Postgres transaction을 담당하는 Hook
    """

    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
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

    def fetch_all(self, sql: str) -> list[tuple]:
        """
        SELECT 계열 SQL 실행 후 전체 결과를 반환합니다
        """
        with closing(super().get_conn()) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()

    def execute(self, sql: str, params: Iterable[Any] | None = None) -> None:
        """
        DDL, MERGE, UPSERT 등과 같은 단일 SQL 문을 실행합니다

        :param params: 단일 sql에 바인딩할 파라미터
        """
        with closing(self.get_conn()) as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def execute_many(self, sql: str, params_list: list[tuple]) -> None:
        """
        동일한 SQL을 여러 row에 대해 반복 실행합니다

        :param params_list: SQL에 바인딩될 파라미터 목록
        """
        with closing(self.get_conn()) as conn:
            try:
                with conn.cursor() as cur:
                    cur.executemany(sql, params_list)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
