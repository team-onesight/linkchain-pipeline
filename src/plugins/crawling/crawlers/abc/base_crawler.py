import logging
from abc import ABC, abstractmethod


class BaseCrawler(ABC):
    log = None
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"  # noqa: E501
    }

    def __init__(self, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def process_crawling(self) -> list[str]:
        raise NotImplementedError("Subclasses must implement this method")
