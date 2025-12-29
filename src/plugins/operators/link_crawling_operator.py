import asyncio
from typing import Any, Dict, Sequence, Type

from airflow.sdk import BaseOperator
from crawling.crawlers.abc.base_crawler import BaseCrawler


class LinkCrawlingOperator(BaseOperator):
    """
    Crawler로 부터 링크들을 수집해서 반환하는 Operator
    :return: Xcom: {return_value: List[str]} # Crawled links
    """

    shallow_copy_attrs: Sequence[str] = (*BaseOperator.shallow_copy_attrs, "crawler")

    template_fields = ("crawler_params",)

    def __init__(
        self,
        crawler_cls: Type[BaseCrawler],
        crawler_params: Dict[str, Any] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.crawler_cls = crawler_cls
        self.crawler_params = crawler_params

    def execute(self, **kwargs) -> Any:
        self.log.info("Crawling started. with Crawler: %s", self.crawler_cls.__name__)

        crawler = self.crawler_cls(**self.crawler_params)

        return asyncio.run(crawler.process_crawling())
