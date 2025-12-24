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
