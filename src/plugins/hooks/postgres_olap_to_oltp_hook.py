from contextlib import closing

from hooks.postgres_base_hook import CustomPostgresBaseHook


class PostgresOlapToOltpHook(CustomPostgresBaseHook):
    """
    OLAP → OLTP(Postgres) 데이터 적재를 위한 Hook

    - connection / transaction / search_path 설정은 CustomPostgresBaseHook에서 처리
    - 실제 cursor 사용 및 SQL 실행은 호출 측(task)에서 수행
    """ # noqa: E501

    def __init__(
        self,
        postgres_conn_id: str = "postgres_dev",
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


    def insert_rows(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        """
        staging 테이블에 rows를 insert하는 메소드
        """
        if not rows:
            self.log.info(f"No rows to insert into {table}")
            return
        
        column_odrer = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table} ({column_odrer}) VALUES ({placeholders})"

        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.executemany(insert_sql, rows)
            conn.commit()

        self.log.info(f"{len(rows)} rows inserted into {table}")


    def upsert_table(self, upsert_sql: str) -> None:
        """
        staging → target 테이블로 upsert하는 메소드
        """
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(upsert_sql)
            conn.commit()

        self.log.info("Upsert completed")
