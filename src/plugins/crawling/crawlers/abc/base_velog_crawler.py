from abc import ABC
from typing import Optional

from crawling.crawlers.abc.base_crawler import BaseCrawler


class BaseVelogCrawler(BaseCrawler, ABC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def _to_url(username: Optional[str], slug: Optional[str]) -> Optional[str]:
        if not username or not slug:
            return None
        return f"https://velog.io/@{username}/{slug}"
