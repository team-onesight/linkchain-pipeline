import pytest


@pytest.fixture(autouse=True)
def mock_snowflake_connection(monkeypatch):
    """
    모든 테스트 실행 시 자동으로 'snowflake_default' 커넥션 환경 변수를 설정합니다.
    형식: AIRFLOW_CONN_{CONN_ID_대문자}
    """
    mock_uri = "snowflake://mock_user:mock_pass@mock_account/LINKCHAIN/RAW_DATA?warehouse=mock_wh&role=mock_role"

    monkeypatch.setenv("AIRFLOW_CONN_SNOWFLAKE_DEFAULT", mock_uri)
