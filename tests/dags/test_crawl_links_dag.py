import crawl_links_dag
import pytest
from airflow.exceptions import AirflowSkipException


@pytest.fixture
def mock_ti(mocker):
    """Airflow TaskInstance Mock"""
    return mocker.MagicMock()


def test_insert_snowflake_success(mocker, mock_ti):
    # given
    source_task_id = "crawl_velog_feed"
    mock_raw_data = ["http://test.com/1", "http://test.com/2"]

    mock_flat_map = mocker.patch.object(crawl_links_dag, "flat_map")
    mock_hash_func = mocker.patch.object(crawl_links_dag, "get_uuid_hash")
    mock_hook_cls = mocker.patch.object(crawl_links_dag, "SnowflakeCommandHook")

    mock_ti.xcom_pull.return_value = mock_raw_data
    mock_flat_map.return_value = mock_raw_data
    mock_hash_func.side_effect = lambda url: f"hash_{url.split('/')[-1]}"

    mock_hook_instance = mock_hook_cls.return_value

    # when
    crawl_links_dag._insert_crawled_links_to_snowflake(
        source_task_id=source_task_id, conn_id="snowflake_default", ti=mock_ti
    )

    # then
    mock_hook_cls.assert_called_with(snowflake_conn_id="snowflake_default")

    mock_hook_instance.command_upsert_transaction.assert_called_once()
    call_args = mock_hook_instance.command_upsert_transaction.call_args.kwargs

    expected_data = [("hash_1", "http://test.com/1"), ("hash_2", "http://test.com/2")]

    assert sorted(call_args["data"]) == sorted(expected_data)
    assert "MERGE INTO LINKCHAIN.RAW_DATA.URL_CRAWLED" in call_args["merge_sql"]
    assert "link_temp_crawl_velog_feed_" in call_args["insert_sql"]


def test_insert_snowflake_skip_no_data(mocker, mock_ti):
    mock_flat_map = mocker.patch.object(crawl_links_dag, "flat_map")

    mock_ti.xcom_pull.return_value = None

    with pytest.raises(AirflowSkipException) as excinfo:
        crawl_links_dag._insert_crawled_links_to_snowflake(
            source_task_id="task_id", ti=mock_ti
        )
    assert "No data" in str(excinfo.value)

    mock_ti.xcom_pull.return_value = [[], []]
    mock_flat_map.return_value = []

    with pytest.raises(AirflowSkipException) as excinfo:
        crawl_links_dag._insert_crawled_links_to_snowflake(
            source_task_id="task_id", ti=mock_ti
        )
    assert "No URLs" in str(excinfo.value)


def test_crawl_velog_feed_wrapper(mocker):
    mock_asyncio_run = mocker.patch.object(crawl_links_dag.asyncio, "run")

    mocker.patch.object(crawl_links_dag, "VelogCrawler")
    mocker.patch.object(crawl_links_dag.aiohttp, "ClientSession")

    # When
    crawl_links_dag._crawl_velog_feed(max_limit=50)

    # Then
    assert mock_asyncio_run.called


def test_crawl_youtube_skip_no_channels(mocker):
    mock_variable_get = mocker.patch.object(crawl_links_dag.Variable, "get")

    mock_variable_get.return_value = []

    # then
    with pytest.raises(AirflowSkipException) as e:
        crawl_links_dag._crawl_youtube(max_limit=50)

    assert "No target channels" in str(e.value)
