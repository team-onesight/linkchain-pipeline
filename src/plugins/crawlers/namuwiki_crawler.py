import logging
from typing import List

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class NamuWikiCrawler:
    """
    나무위키 최근 변경 내역 크롤러
    """

    BASE_URL = "https://namu.wiki"
    RECENT_CHANGES_URL = "https://namu.wiki/RecentChanges"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"  # noqa: E501
    }

    CSS_SELECTOR = "div.ajtzPLeO.b8dd3F0y > a"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_recent_changes(self) -> List[str]:
        href_list = []
        logger.info(f"--- 나무위키 수집 시작: {self.RECENT_CHANGES_URL} ---")

        try:
            async with self.session.get(
                self.RECENT_CHANGES_URL, headers=self.HEADERS, timeout=10
            ) as response:
                response.raise_for_status()
                html_content = await response.text()

            soup = BeautifulSoup(html_content, "html.parser")
            elements = soup.select(self.CSS_SELECTOR)

            if not elements:
                logger.warning(
                    f"요소를 찾을 수 없습니다. (Selector: '{self.CSS_SELECTOR}') "
                    "나무위키 프론트엔드 업데이트로 클래스명이 변경되었을 수 있습니다."
                )
                return []

            for element in elements:
                href = element.get("href")
                if href and href.startswith("/w/"):
                    full_url = f"{self.BASE_URL}{href}"
                    href_list.append(full_url)

            logger.info(f"나무위키 수집 완료: {len(href_list)}개 URL")

        except aiohttp.ClientError as e:
            logger.error(f"나무위키 네트워크 오류: {e}")
        except Exception as e:
            logger.error(f"나무위키 파싱 중 오류: {e}")

        return href_list
