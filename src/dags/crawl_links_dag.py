import dataclasses as dc
import logging
from typing import Any, Dict, Type

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup, TriggerRule
from crawling.crawlers.abc.base_crawler import BaseCrawler
from crawling.crawlers.namuwiki_crawler import NamuWikiCrawler
from crawling.crawlers.naver_news_crawler import NaverNewsCrawler
from crawling.crawlers.velog_crawlers import (
    VelogFeedCrawler,
    VelogPostType,
    VelogTrendingCrawler,
    VelogTrendingTimeframe,
)
from crawling.crawlers.youtube_crawler import YoutubeCrawler
from operators.link_crawling_operator import LinkCrawlingOperator
from operators.snowflake_upsert_links_operator import SnowflakeUpsertLinksOperator

logger = logging.getLogger(__name__)


@dc.dataclass
class CrawlerConfig:
    task_id: str
    crawler_cls: Type[BaseCrawler]  # crawler class
    crawler_params: Dict[str, Any] = dc.field(
        default_factory=dict
    )  # crawler init params


crawler_configs: list[CrawlerConfig] = [
    CrawlerConfig(
        task_id="crawl_velog_feed",
        crawler_cls=VelogFeedCrawler,
        crawler_params={
            "post_types": [VelogPostType.RECENT, VelogPostType.RECENT],
            "max_limit": 100,
        },
    ),
    CrawlerConfig(
        task_id="crawl_velog_trending",
        crawler_cls=VelogTrendingCrawler,
        crawler_params={
            "timeframes": [
                VelogTrendingTimeframe.DAY,
                VelogTrendingTimeframe.WEEK,
                VelogTrendingTimeframe.MONTH,
            ],
            "max_limit": 100,
        },
    ),
    CrawlerConfig(task_id="crawl_namuwiki", crawler_cls=NamuWikiCrawler),
    CrawlerConfig(
        task_id="crawl_naver_news",
        crawler_cls=NaverNewsCrawler,
        crawler_params={
            "news_sectors": "{{ var.json.get('naver_news_sections', []) }}",
            "max_limit": 100,
        },
    ),
    CrawlerConfig(
        task_id="crawl_youtube",
        crawler_cls=YoutubeCrawler,
        crawler_params={
            "channels": "{{ var.json.get('youtube_target_channels', []) }}",
            "max_limit": 50,
        },
    ),
]

with DAG(
    dag_id="crawl_links_dag",
    schedule="@hourly",
    render_template_as_native_obj=True,
    start_date=None,
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    with TaskGroup("crawl_links_tasks") as crawl_links_tasks_group:
        for cfg in crawler_configs:
            crawl_task = LinkCrawlingOperator(
                task_id=cfg.task_id,
                crawler_cls=cfg.crawler_cls,
                crawler_params=cfg.crawler_params,
            )

            insert_task = SnowflakeUpsertLinksOperator(
                task_id=f"insert_{cfg.task_id}",
                source_task_id=crawl_task.task_id,
                links_xcom_key="return_value",
            )

            crawl_task >> insert_task

    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.NONE_FAILED)

    start_task >> crawl_links_tasks_group >> end_task


if __name__ == "__main__":
    dag.test()
