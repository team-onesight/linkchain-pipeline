import asyncio

import pytest
from airflow.exceptions import AirflowSkipException
from crawling.crawlers.youtube_crawler import YoutubeCrawler
from operators.link_crawling_operator import LinkCrawlingOperator
from operators.snowflake_upsert_links_operator import SnowflakeUpsertLinksOperator


@pytest.fixture
def mock_context(mocker):
    ti = mocker.MagicMock()
    return {"ti": ti}


def test_snowflake_upsert_operator_success(mocker, mock_context):
    # given
    source_task_id = "crawl_velog_feed"
    mock_ti = mock_context["ti"]
    mock_raw_data = ["http://test.com/1", "http://test.com/2"]

    mocker.patch(
        "operators.snowflake_upsert_links_operator.flat_map", return_value=mock_raw_data
    )
    mocker.patch(
        "operators.snowflake_upsert_links_operator.get_uuid_hash",
        side_effect=lambda url: f"hash_{url.split('/')[-1]}",
    )
    mock_hook_cls = mocker.patch(
        "operators.snowflake_upsert_links_operator.SnowflakeCommandHook"
    )
    mock_hook_instance = mock_hook_cls.return_value

    mock_ti.xcom_pull.return_value = mock_raw_data

    # when
    op = SnowflakeUpsertLinksOperator(
        task_id="test_insert",
        source_task_id=source_task_id,
        conn_id="snowflake_default",
    )
    op.execute(context=mock_context)

    # then
    mock_hook_cls.assert_called_with(snowflake_conn_id="snowflake_default")
    mock_hook_instance.command_upsert_transaction.assert_called_once()

    call_args = mock_hook_instance.command_upsert_transaction.call_args.kwargs
    expected_data = [("hash_1", "http://test.com/1"), ("hash_2", "http://test.com/2")]

    assert sorted(call_args["data"]) == sorted(expected_data)
    assert "MERGE INTO LINKCHAIN.RAW_DATA.URL_CRAWLED" in call_args["merge_sql"]


def test_snowflake_upsert_operator_skip(mocker, mock_context):
    mock_ti = mock_context["ti"]
    op = SnowflakeUpsertLinksOperator(task_id="test_skip", source_task_id="any")

    # then
    mock_ti.xcom_pull.return_value = None
    with pytest.raises(AirflowSkipException, match="No data found"):
        op.execute(context=mock_context)

    mock_ti.xcom_pull.return_value = [[], []]
    mocker.patch("operators.snowflake_upsert_links_operator.flat_map", return_value=[])
    with pytest.raises(AirflowSkipException, match="No valid URLs"):
        op.execute(context=mock_context)


def test_link_crawling_operator_execute(mocker, mock_context):
    # given
    mock_instance = mocker.MagicMock()
    mock_instance.requires_session = True
    mock_instance.process_crawling = mocker.AsyncMock(return_value=["http://link1.com"])
    mock_crawler_cls = mocker.MagicMock(return_value=mock_instance)
    mock_crawler_cls.__name__ = "MockCrawler"
    test_params = {"max_limit": 50}
    op = LinkCrawlingOperator(
        task_id="test_crawl",
        crawler_cls=mock_crawler_cls,
        crawler_params=test_params,
    )

    # when
    result = op.execute(context=mock_context)

    # then
    assert result == ["http://link1.com"]
    mock_crawler_cls.assert_called_once_with(**test_params)
    mock_instance.process_crawling.assert_called_once()


def test_youtube_crawler_no_channels_error_handling(mocker):
    # given
    mock_var_get = mocker.patch("airflow.models.Variable.get")
    mock_var_get.return_value = []

    mock_var_get = mocker.patch("crawling.crawlers.youtube_crawler.Variable.get")

    mock_var_get.side_effect = Exception("Variable not found")

    crawler = YoutubeCrawler(channels_variable_key="test_key")

    # then
    with pytest.raises(AirflowSkipException) as excinfo:
        asyncio.run(crawler.process_crawling())

    assert "채널 정보 없음" in str(excinfo.value)
    mock_var_get.assert_called_with("test_key", deserialize_json=True)
