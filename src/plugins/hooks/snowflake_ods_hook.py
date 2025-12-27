from hooks.abc.snowflake_base_hook import CustomSnowflakeBaseHook


class SnowflakeODSQueryHook(CustomSnowflakeBaseHook):
    """
    SnowflakeODSQueryHook은 snowflake 데이터베이스에 ODS 스키마를 로드하기 위한 커스텀 훅입니다.
    쿼리를 실행하고 결과를 받을 때 사용하시기 바랍니다.

    :param snowflake_conn_id: The Airflow connection ID to use for Snowflake.
    """  # noqa: E501

    def __init__(self, *args, snowflake_conn_id: str = "snowflake_default", **kwargs):
        super().__init__(
            *args,
            snowflake_conn_id=snowflake_conn_id,
            database="LINKCHAIN",
            schema="ODS",
            **kwargs,
        )

    def get_links(self, table_name: str, columns: list[str], chunk_size: int):
        """
        Snowflake ODS 테이블의 데이터를 청크(Chunk) 단위로 읽어와 Pandas DataFrame 제너레이터로 반환합니다.

        1. Snowflake Connection을 통해 데이터를 쿼리합니다.
        2. 지정된 chunk_size 만큼 레코드를 순차적으로 가져옵니다.
        3. 가져온 레코드를 Pandas DataFrame으로 변환하고, 혼동을 방지하기 위해 컬럼명을 대문자로 정규화합니다.
        4. 제너레이터(Generator)를 통해 변환된 DataFrame을 순차적으로 yield 하여 제공합니다.

        :param table_name: 데이터를 가져올 Snowflake 테이블 이름
        :param columns: 조회할 컬럼 리스트
        :param chunk_size: 한 번의 루프에서 가져올 행(row)의 개수
        :return: Pandas DataFrame 객체를 순차적으로 반환하는 Generator
        """ # noqa: E501
        self.log.info(f"Fetching data from {table_name}...")
        with closing(self.get_conn()) as conn:
            with conn.cursor() as cursor:
                # 1
                query = f"SELECT {', '.join(columns)} FROM {table_name}"  # noqa: S608
                cursor.execute(query)

                while True:
                    # 2
                    records = cursor.fetchmany(chunk_size)
                    if not records:
                        break
                    # 3
                    df = pd.DataFrame(records)
                    df.columns = [c.upper() for c in columns]
                    self.log.info(f"Fetched a chunk of size {len(df)}")

                    # 4
                    yield df
