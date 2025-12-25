import asyncio
from typing import List, Tuple

import aiohttp
from airflow.sdk import Variable
from bs4 import BeautifulSoup
from crawling.crawlers.abc.base_crawler import BaseCrawler


class NaverNewsCrawler(BaseCrawler):
    ENTER_SECTION = "106"
    SPORT_SECTION = "107"
    BASE_URL = "https://news.naver.com/section"
    sections: List[str] = [ENTER_SECTION, SPORT_SECTION]
    sector_variable_key = None

    def __init__(self, sector_variable_key: str = "naver_news_sections", **kwargs):
        super().__init__(**kwargs)

        self.sector_variable_key = sector_variable_key

    async def process_crawling(self) -> List[str]:
        if not self.sections:
            return []
        if self.sector_variable_key:
            dynamic_sections = Variable.get(
                self.sector_variable_key, deserialize_json=True, default=[]
            )
            self.sections = list(set(self.sections + dynamic_sections))

        async with aiohttp.ClientSession() as session:
            tasks = [self._crawl_section(sec_id, session) for sec_id in self.sections]
            results = await asyncio.gather(*tasks)

            total_links = [link for res in results for link in res]
            return total_links

    def _get_section_info(self, section_id: str) -> Tuple[str, str]:
        if section_id == self.ENTER_SECTION:
            return "https://m.entertain.naver.com/ranking", "#content a"
        elif section_id == self.SPORT_SECTION:
            return "https://sports.news.naver.com/ranking/index", ".content a.title"
        else:
            return f"{self.BASE_URL}/{section_id}", "a.sa_text_title"

    async def _crawl_section(
        self, section_id: str, session: aiohttp.ClientSession
    ) -> List[str]:
        links = []
        url, selector = self._get_section_info(section_id)

        try:
            async with session.get(url, headers=self.HEADERS, timeout=10) as response:
                response.raise_for_status()
                html = await response.text()

            soup = BeautifulSoup(html, "html.parser")
            elements = soup.select(selector)

            if not elements:
                self.log.warning(
                    f"[Zero] Section {section_id}: 요소를 찾을 수 없습니다. (URL: {url})"  # noqa: E501
                )
                return []

            for element in elements:
                href = element.get("href")
                if href:
                    links.append(href)

            self.log.info(f"[Done] Section {section_id}: {len(links)}개 수집")

        except Exception as e:
            self.log.error(f"[Error] Section {section_id}: {e}")

        return links
