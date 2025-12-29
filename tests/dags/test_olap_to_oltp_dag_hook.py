from unittest.mock import MagicMock

from hooks.postgres_olap_to_oltp_hook import PostgresOlapToOltpHook
from hooks.snowflake_analytics_hook import SnowflakeAnalyticsQueryHook


# mock connection and cursor setup
def make_mock_conn_and_cursor():
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.cursor.return_value = mock_cursor

    return mock_conn, mock_cursor


# PostgresOlapToOltpHook tests
def test_truncate_and_insert_rows_with_data():
    """
    TRUNCATE + INSERT test
    """
    hook = PostgresOlapToOltpHook()
    mock_conn, mock_cursor = make_mock_conn_and_cursor()
    hook.get_conn = MagicMock(return_value=mock_conn)

    rows = [(1, "a"), (2, "b")]
    hook.truncate_and_insert_rows("staging.links", ["id", "url"], rows)

    mock_cursor.execute.assert_called_once_with("TRUNCATE TABLE staging.links")
    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO staging.links (id,url) VALUES (%s,%s)", rows
    )
    mock_conn.commit.assert_called_once()

def test_upsert_table_calls_execute_and_commit():
    """
    staging → target upsert 테스트
    """
    hook = PostgresOlapToOltpHook()
    mock_conn, mock_cursor = make_mock_conn_and_cursor()
    hook.get_conn = MagicMock(return_value=mock_conn)

    upsert_sql = "MERGE INTO public.links ..."
    hook.upsert_table(upsert_sql)

    mock_cursor.execute.assert_called_once_with(upsert_sql)
    mock_conn.commit.assert_called_once()


# SnowflakeAnalyticsQueryHook test
def test_get_olap_table_data():
    """
    OLAP 데이터 가져오기 테스트
    """
    hook = SnowflakeAnalyticsQueryHook()
    mock_conn, mock_cursor = make_mock_conn_and_cursor()
    mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]
    hook.get_conn = MagicMock(return_value=mock_conn)

    columns, rows = hook.get_olap_table_data("olap.links", ["id", "url"])

    expected_sql = "SELECT id, url FROM olap.links"
    mock_cursor.execute.assert_called_once_with(expected_sql)
    assert columns == ["id", "url"]
    assert rows == [(1, "a"), (2, "b")]
