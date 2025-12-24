from hooks.postgres_base_hook import CustomPostgresBaseHook


class PostgresOltpToOlapHook(CustomPostgresBaseHook):
    """
    OLTP(Postgres) → OLAP 적재용 데이터를 읽기 위한 Hook

    - BaseHook에서 connection 재사용 및 search_path 관리
    - 실제 select / extract 로직은 task 단에서 구현
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
