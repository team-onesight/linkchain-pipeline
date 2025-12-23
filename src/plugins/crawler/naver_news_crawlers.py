import asyncio
import logging
from typing import List, Tuple

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class NaverNewsCrawler:
    ENTER_SECTION = "106"
    SPORT_SECTION = "107"
    BASE_URL = "https://news.naver.com/section"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"  # noqa: E501
    }

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    def _get_section_info(self, section_id: str) -> Tuple[str, str]:
        if section_id == self.ENTER_SECTION:
            return "https://m.entertain.naver.com/ranking", "#content a"
        elif section_id == self.SPORT_SECTION:
            return "https://sports.news.naver.com/ranking/index", ".content a.title"
        else:
            return f"{self.BASE_URL}/{section_id}", "a.sa_text_title"

    async def _crawl_section(self, section_id: str) -> List[str]:
        links = []
        url, selector = self._get_section_info(section_id)

        try:
            async with self.session.get(
                url, headers=self.HEADERS, timeout=10
            ) as response:
                response.raise_for_status()
                html = await response.text()

            soup = BeautifulSoup(html, "html.parser")
            elements = soup.select(selector)

            if not elements:
                logger.warning(
                    f"[Zero] Section {section_id}: 요소를 찾을 수 없습니다. (URL: {url})"
                )
                return []

            for element in elements:
                href = element.get("href")
                if href:
                    links.append(href)

            logger.info(f"[Done] Section {section_id}: {len(links)}개 수집")

        except Exception as e:
            logger.error(f"[Error] Section {section_id}: {e}")

        return links

    async def crawl(self, sections: List[str]) -> List[str]:
        if not sections:
            return []

        tasks = [self._crawl_section(sec_id) for sec_id in sections]
        results = await asyncio.gather(*tasks)

        total_links = [link for res in results for link in res]
        return total_links
