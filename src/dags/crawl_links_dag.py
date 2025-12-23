import asyncio
import dataclasses as dc
import logging
import uuid
from typing import Any, Callable, Dict, Iterable, Optional

import aiohttp
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup, TriggerRule
from common.hash_utils import get_uuid_hash
from common.iterable_utils import flat_map
from crawler.namuwiki_crawlers import NamuWikiCrawler
from crawler.naver_news_crawlers import NaverNewsCrawler
from crawler.velog_crawlers import VelogCrawler, VelogPostType, VelogTrendingTimeframe
from crawler.youtube_crawlers import crawl_channels
from hooks.snowflake_command_hook import SnowflakeCommandHook

logger = logging.getLogger(__name__)


def _crawl_velog_feed(max_limit: int = 100, **kwargs):
    """
    Velog 최신 글 및 추천 글 크롤링
    """

    async def _async_logic():
        async with aiohttp.ClientSession() as session:
            crawler = VelogCrawler(session)
            tasks = [
                crawler.get_feed_posts(VelogPostType.RECENT, max_limit=max_limit),
                crawler.get_feed_posts(VelogPostType.CURATED, max_limit=max_limit),
            ]
            return await asyncio.gather(*tasks)

    logger.info("Starting Velog Feed Crawl...")
    return asyncio.run(_async_logic())


def _crawl_velog_trending(max_limit: int, **kwargs):
    """
    Velog 인기 글 크롤링
    """

    async def _async_logic():
        async with aiohttp.ClientSession() as session:
            crawler = VelogCrawler(session)
            tasks = [
                crawler.get_trending_posts(
                    max_limit=max_limit, timeframe=VelogTrendingTimeframe.DAY
                ),
                crawler.get_trending_posts(
                    max_limit=max_limit, timeframe=VelogTrendingTimeframe.MONTH
                ),
                crawler.get_trending_posts(
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
    targets = Variable.get(
        "youtube_target_channels", deserialize_json=True, default_var=[]
    )

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
        "naver_news_sections", deserialize_json=True, default_var=["105"]
    )

    if not sections:
        logger.warning("No sections found. Skipping Naver task.")
        raise AirflowSkipException("Skipping Naver: No sections defined.")

    async def _async_logic():
        async with aiohttp.ClientSession() as session:
            crawler = NaverNewsCrawler(session)
            return await crawler.crawl(sections=sections)

    logger.info(f"Starting Naver News Crawl for sections: {sections}")
    return asyncio.run(_async_logic())


def _crawl_namuwiki(**kwargs):
    """
    Namuwiki 크롤링
    """

    async def _async_logic():
        async with aiohttp.ClientSession() as session:
            crawler = NamuWikiCrawler(session)
            return await crawler.get_recent_changes()

    logger.info("Starting Namuwiki Crawl...")
    return asyncio.run(_async_logic())


def _insert_crawled_links_to_snowflake(
    source_task_id: str,
    conn_id: str = "snowflake_default",
    **kwargs,
):
    """
    Save crawled links to Snowflake DWH
    :param source_task_id: up-stream task ID that provides URLs through XCom (return_value)
    :type source_task_id:
    :param conn_id:
    :type conn_id:
    :param kwargs:
    :type kwargs:
    :return:
    :rtype:
    """
    ti = kwargs["ti"]
    raw_data = ti.xcom_pull(task_ids=source_task_id, key="return_value")

    urls = set(flat_map(None, raw_data))

    if not urls:
        logger.warning(f"[{source_task_id}] No URLs to save.")
        raise AirflowSkipException("No URLs to save.")

    data_to_insert = []
    for url in urls:
        if url and isinstance(url, str):
            link_id = get_uuid_hash(url)
            data_to_insert.append((link_id, url))

    if not data_to_insert:
        raise AirflowSkipException("No valid URLs found after processing.")

    hook = SnowflakeCommandHook(snowflake_conn_id=conn_id)
    target_table = "URL_CRAWLED"
    temp_table = f"link_temp_{source_task_id}_{uuid.uuid4().hex[:8]}"

    try:
        logger.info(f"Creating temp table: {temp_table}")
        create_temp_sql = f"""
            CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table} (
                link_id     VARCHAR,
                url         VARCHAR(2048),
                created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
        """
        hook.run(create_temp_sql)

        logger.info(f"Inserting {len(data_to_insert)} rows into {temp_table}")
        hook.insert_rows(
            table=temp_table,
            rows=data_to_insert,
            target_fields=["link_id", "url"],
        )

        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_table} AS source
            ON target.link_id = source.link_id

            WHEN MATCHED THEN
                UPDATE SET 
                    target.updated_at = source.created_at

            WHEN NOT MATCHED THEN
                INSERT (link_id, url, created_at, updated_at)
                VALUES (source.link_id, source.url, source.created_at, source.created_at)
        """

        hook.run(merge_sql)
        logger.info(f"MERGE completed for {target_table}")

    except Exception as e:
        logger.error(f"Snowflake Error: {e}")
        raise

    finally:
        hook.run(f"DROP TABLE IF EXISTS {temp_table}")


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
    schedule="@once",
    start_date=None,
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    with TaskGroup("crawl_links_tasks") as crawl_links_tasks_group:
        for idx, crawler in enumerate(crawler_configs):
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
