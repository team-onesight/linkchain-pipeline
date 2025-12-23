import asyncio
import dataclasses as dc
import logging
import uuid
from typing import Any, Callable, Dict, Iterable, Optional

import aiohttp
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup, TriggerRule, Variable
from common.hash_utils import get_uuid_hash
from common.iterable_utils import flat_map
from crawlers.namuwiki_crawlers import NamuWikiCrawler
from crawlers.naver_news_crawlers import NaverNewsCrawler
from crawlers.velog_crawlers import VelogCrawler, VelogPostType, VelogTrendingTimeframe
from crawlers.youtube_crawlers import crawl_channels
from hooks.snowflake_command_hook import SnowflakeCommandHook

logger = logging.getLogger(__name__)


def _crawl_velog_feed(max_limit: int = 100):
    """
    Velog 최신 글 및 추천 글 크롤링
    """

    async def _async_logic():
        async with aiohttp.ClientSession() as session:
            velog_crawler = VelogCrawler(session)
            tasks = [
                velog_crawler.get_feed_posts(VelogPostType.RECENT, max_limit=max_limit),
                velog_crawler.get_feed_posts(
                    VelogPostType.CURATED, max_limit=max_limit
                ),
            ]
            return await asyncio.gather(*tasks)

    logger.info("Starting Velog Feed Crawl...")
    return asyncio.run(_async_logic())


def _crawl_velog_trending(max_limit: int):
    """
    Velog 인기 글 크롤링
    """

    async def _async_logic():
        async with aiohttp.ClientSession() as session:
            velog_crawler = VelogCrawler(session)
            tasks = [
                velog_crawler.get_trending_posts(
                    max_limit=max_limit, timeframe=VelogTrendingTimeframe.DAY
                ),
                velog_crawler.get_trending_posts(
                    max_limit=max_limit, timeframe=VelogTrendingTimeframe.MONTH
                ),
                velog_crawler.get_trending_posts(
                    max_limit=max_limit, timeframe=VelogTrendingTimeframe.WEEK
                ),
            ]
            return await asyncio.gather(*tasks)

    logger.info("Starting Velog Trending Crawl...")
    return asyncio.run(_async_logic())


def _crawl_youtube(max_limit: int, **kwargs):
    """
    YouTube 크롤링
    """
    targets = Variable.get("youtube_target_channels", deserialize_json=True)

    if not targets:
        logger.warning("No target channels found. Skipping YouTube task.")
        raise AirflowSkipException("Skipping YouTube: No target channels defined.")

    async def _async_logic():
        return await crawl_channels(targets, max_limit=max_limit)

    logger.info(f"Starting YouTube Crawl for {len(targets)} channels...")
    return asyncio.run(_async_logic())


def _crawl_naver(**kwargs):
    """
    Naver 뉴스 크롤링
    """
    sections = Variable.get(
        "naver_news_sections", deserialize_json=True, default=["105"]
    )

    if not sections:
        logger.warning("No sections found. Skipping Naver task.")
        raise AirflowSkipException("Skipping Naver: No sections defined.")

    async def _async_logic():
        async with aiohttp.ClientSession() as session:
            naver_news_crawler = NaverNewsCrawler(session)
            return await naver_news_crawler.crawl(sections=sections)

    logger.info(f"Starting Naver News Crawl for sections: {sections}")
    return asyncio.run(_async_logic())


def _crawl_namuwiki(**kwargs):
    """
    Namuwiki 크롤링
    """

    async def _async_logic():
        async with aiohttp.ClientSession() as session:
            namu_crawler = NamuWikiCrawler(session)
            return await namu_crawler.get_recent_changes()

    logger.info("Starting Namuwiki Crawl...")
    return asyncio.run(_async_logic())


def _insert_crawled_links_to_snowflake(
    source_task_id: str,
    conn_id: str = "snowflake_default",
    **kwargs,
):
    """
    insert Crawled Links not Duplicated to Snowflake Table (URL_CRAWLED) with LINK_ID(hash)
    though MERGE INTO Query

    if there is no data or urls(flat_map(data)), raise AirflowSkipException
    if there is already existing link, update the updated_at column

    expected: insert urls from XCom of source_task_id to Snowflake without duplication & hashing id

    :param source_task_id: upstream task id which returns crawled links list
    :param conn_id: snowflake connection id
    :param kwargs: required by PythonOperator
    """  # noqa: E501
    ti = kwargs["ti"]

    raw_data = ti.xcom_pull(task_ids=source_task_id, key="return_value")
    if not raw_data:
        raise AirflowSkipException("No data.")

    urls = set(flat_map(None, raw_data))
    if not urls:
        raise AirflowSkipException("No URLs.")

    data_to_insert = [
        (get_uuid_hash(url), url) for url in urls if url and isinstance(url, str)
    ]

    safe_task_id = source_task_id.replace(".", "_")
    database = "LINKCHAIN"
    schema = "RAW_DATA"
    target_table = f"{database}.{schema}.URL_CRAWLED"
    temp_table = f"{database}.{schema}.link_temp_{safe_task_id}_{uuid.uuid4().hex[:8]}"

    create_sql = f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table} (
            link_id     VARCHAR,
            url         VARCHAR(2048),
            created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        )
    """

    insert_sql = f"INSERT INTO {temp_table} (link_id, url) VALUES (%s, %s)"  # noqa: S608

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_table} AS source
        ON target.link_id = source.link_id
        WHEN MATCHED THEN
            UPDATE SET target.updated_at = source.created_at
        WHEN NOT MATCHED THEN
            INSERT (link_id, url, created_at)
            VALUES (source.link_id, source.url, source.created_at)
    """  # noqa: S608

    hook = SnowflakeCommandHook(snowflake_conn_id=conn_id)

    hook.command_upsert_transaction(
        create_temp_table_sql=create_sql,
        insert_sql=insert_sql,
        merge_sql=merge_sql,
        data=data_to_insert,
        temp_table_name=temp_table,
    )


@dc.dataclass(unsafe_hash=True)
class CrawlerConfig:
    task_id: str
    callable_func: Callable
    op_kwargs: Optional[Dict[str, Any]] = None


crawler_configs: Iterable[CrawlerConfig] = [
    CrawlerConfig(
        "crawl_velog_trending",
        _crawl_velog_trending,
        {"max_limit": 100},
    ),
    CrawlerConfig("crawl_velog_feed", _crawl_velog_feed, {"max_limit": 100}),
    CrawlerConfig("crawl_youtube", _crawl_youtube, {"max_limit": 100}),
    CrawlerConfig("crawl_naver", _crawl_naver, None),
    CrawlerConfig("crawl_namuwiki", _crawl_namuwiki, None),
]

with DAG(
    dag_id="crawl_links_dag",
    schedule="@hourly",
    start_date=None,
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    with TaskGroup("crawl_links_tasks") as crawl_links_tasks_group:
        for crawler in crawler_configs:
            crawl_task = PythonOperator(
                task_id=crawler.task_id,  # 예: "crawl_velog_feed"
                python_callable=crawler.callable_func,
                op_kwargs=crawler.op_kwargs or {},
            )

            insert_task = PythonOperator(
                task_id=f"insert_{crawler.task_id}",
                python_callable=_insert_crawled_links_to_snowflake,
                op_kwargs={
                    "source_task_id": crawl_task.task_id,
                    "primary_key": "link_id",
                },
            )

            crawl_task >> insert_task

    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.NONE_FAILED)

    start_task >> crawl_links_tasks_group >> end_task


if __name__ == "__main__":
    dag.test()
