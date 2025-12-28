from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from operators.parsing_link_detail_operator import ParseLinkDetailOperator
from operators.postgres_to_snowflake_operator import PostgresToSnowflakeOperator


@pytest.fixture
def mock_context():
    """
    Airflow Operator의 execute 메서드에 전달되는 context 인자를 모사하는 fixture입니다.
    """
    return {}


#######################
######## Mocking Hook ###
#########################
"""
Hook들을 Mocking 합니다.
"""
@pytest.fixture
def mock_pg_hook():
    with patch(
        "operators.postgres_to_snowflake_operator.PostgresOltpToOlapHook"
    ) as mock:
        yield mock


@pytest.fixture
def mock_sf_command_hook():
    with patch(
        "operators.postgres_to_snowflake_operator.SnowflakeCommandHook"
    ) as mock:
        yield mock


@pytest.fixture
def mock_sf_command_hook_for_parsing():
    with patch("operators.parsing_link_detail_operator.SnowflakeCommandHook") as mock:
        yield mock


@pytest.fixture
def mock_sf_ods_query_hook():
    with patch(
        "operators.parsing_link_detail_operator.SnowflakeODSQueryHook"
    ) as mock:
        yield mock
###########################

def test_postgres_to_snowflake_operator(
    mock_context, mock_pg_hook, mock_sf_command_hook
):
    """
    Postgres 데이터를 Parquet로 추출하여 Snowflake Stage를 거쳐 적재하는 전체 흐름을 테스트합니다.

    검증 항목:
    1. Postgres Hook이 올바른 Connection ID로 생성되었는가
    2. 데이터 추출 함수(bulk_dump)가 설정된 인자값으로 호출되었는가
    3. Snowflake 적재를 위한 SQL 명령어(TRUNCATE -> CREATE STAGE -> PUT -> COPY -> DROP STAGE)가
       의도한 순서대로 정확하게 실행되었는가
    4. 로컬에 생성된 임시 Parquet 파일이 정상적으로 삭제(os.remove) 되었는가
    """  # noqa: E501

    # given
    table_key = "test_table"
    table_config = {
        "source_table": "public.test_source",
        "snowflake_table": "ods.test_dest",
        "columns": ["id", "value"],
    }
    chunk_size = 100
    snowflake_db = "linkchain"
    snowflake_schema = "ods"
    tmp_file_path = "/tmp/test.parquet"  # noqa: S108

    op = PostgresToSnowflakeOperator(
        task_id="test_task",
        postgres_conn_id="postgres_default",
        snowflake_conn_id="snowflake_default",
        table_key=table_key,
        table_config=table_config,
        chunk_size=chunk_size,
        snowflake_db=snowflake_db,
        snowflake_schema=snowflake_schema,
    )

    mock_pg_hook_instance = mock_pg_hook.return_value
    mock_pg_hook_instance.bulk_dump_to_parquet_files.return_value = [tmp_file_path]

    mock_sf_hook_instance = mock_sf_command_hook.return_value
    mock_sf_hook_instance.command.return_value = [
        ['col1', 'col2', 'col3', 'col4', 'col5']
        ]

    # when
    with patch("os.path.abspath", return_value=tmp_file_path), patch(
        "os.remove"
    ) as mock_remove:
        op.execute(mock_context)

    # then
    # 1
    mock_pg_hook.assert_called_with(postgres_conn_id="postgres_default")
    mock_sf_command_hook.assert_called_with(
        snowflake_conn_id="snowflake_default",
        database=snowflake_db,
        schema=snowflake_schema,
    )

    # 2
    mock_pg_hook_instance.bulk_dump_to_parquet_files.assert_called_with(
        table_name=table_config["source_table"],
        columns=table_config["columns"],
        chunk_size=chunk_size,
    )

    # 3
    stage_name = f"{snowflake_db}.{snowflake_schema}.temp_stage_public_test_source"
    expected_sf_commands = [
        call(f"TRUNCATE TABLE {table_config['snowflake_table']}"),
        call(f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT = (TYPE = 'PARQUET')"),
        call(f"PUT 'file://{tmp_file_path}' @{stage_name}"),
        call(
            f"""
                    COPY INTO {table_config['snowflake_table']}
                    FROM @{stage_name}
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    """
        ),
        call(f"DROP STAGE IF EXISTS {stage_name}"),
    ]
    mock_sf_hook_instance.command.assert_has_calls(
        expected_sf_commands, any_order=False
    )

    # 4
    mock_remove.assert_called_with(tmp_file_path)


def test_parse_link_detail_operator(
    mock_context, mock_sf_ods_query_hook, mock_sf_command_hook_for_parsing
):
    """
    Snowflake ods schema의 원본 데이터를 읽어 파싱한 후, 타겟 테이블에 대량 삽입(executemany)을 테스트합니다.

    검증 항목:
    1. 데이터를 잘 읽어오는가
    2. TRUNCATE 명령이 실행되는가
    3. URL에서 parse된 정보(subdomain, host 등)가 올바른가
    4. 정상적으로 전달되고 커밋/종료되는가
    """  # noqa: E501
    # given
    op = ParseLinkDetailOperator(
        task_id="test_parse_link_detail",
        snowflake_db="linkchain",
        snowflake_conn_id="snowflake_default",
        source_table="ods.link",
        source_columns=["link_id", "url"],
        dest_table="raw_data.link_detail",
        dest_columns=["link_id", "subdomain", "host", "path", "parameters", "fragment"],
        chunk_size=100,
    )

    mock_ods_hook_instance = mock_sf_ods_query_hook.return_value
    df_chunk = pd.DataFrame(
        [
            (1, "https://www.google.com/search?q=test"),
            (2, "https://sub.test.co.uk/page#section"),
        ],
        columns=["LINK_ID", "URL"],
    )
    mock_ods_hook_instance.query.return_value = [df_chunk]

    mock_cmd_hook_instance = mock_sf_command_hook_for_parsing.return_value
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cmd_hook_instance.get_conn.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # when
    op.execute(mock_context)

    # then
    # 1
    mock_sf_ods_query_hook.assert_called_with(snowflake_conn_id="snowflake_default")
    mock_ods_hook_instance.query.assert_called_with(
        "ods.link", ["link_id", "url"], 100
    )

    # 2
    mock_sf_command_hook_for_parsing.assert_called_with(
        snowflake_conn_id="snowflake_default",
        database="linkchain",
        schema="raw_data",
    )
    mock_cmd_hook_instance.command.assert_called_with("TRUNCATE TABLE link_detail")

    # 3
    expected_records = [
        (1, "www", "google.com", "/search", "q=test", None),
        (2, "sub", "test.co.uk", "/page", None, "section"),
    ]

    args, kwargs = mock_cursor.executemany.call_args
    assert "INSERT INTO link_detail" in args[0]
    assert args[1] == expected_records

    # 4
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@pytest.mark.parametrize(
    "url, expected",
    [
        (
            "https://www.google.com/search?q=test",
            ("www", "google.com", "/search", "q=test", None),
        ),
        (
            "https://sub.test.co.uk/page#section",
            ("sub", "test.co.uk", "/page", None, "section")
        ),
        (
            "http://localhost:8080",
            (None, "localhost", None, None, None)
        ),
        (
            "invalid-url",
            (None, None, None, None, None)
        ),
    ],
)
def test_parse_url_details(url, expected):
    """
    ParseLinkDetailOperator 내의 핵심 로직인 URL 파싱 함수를 다양한 케이스별로 검증합니다.

    검증 케이스:
    - 표준 HTTPS URL
    - 서브도메인이 포함된 복잡한 URL
    - 포트 번호가 포함된 로컬 호스트 URL
    - 유효하지 않은 형식의 문자열 입력 시 처리 결과
    """  # noqa: E501
    # given
    op = ParseLinkDetailOperator(
        task_id="test_parse_url_details",
        snowflake_db="",
        snowflake_conn_id="",
        source_table="",
        source_columns=[],
        dest_table="",
        dest_columns=[],
        chunk_size=0,
    )

    # when
    result = op.parse_url_details(url)

    # then
    assert result == expected
