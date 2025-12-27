from typing import List

import aiohttp
from airflow.exceptions import AirflowSkipException
from bs4 import BeautifulSoup
from crawling.crawlers.abc.base_crawler import BaseCrawler


class NamuWikiCrawler(BaseCrawler):
    """
    나무위키 최근 변경 내역 크롤러
    """

    BASE_URL = "https://namu.wiki"
    RECENT_CHANGES_URL = "https://namu.wiki/RecentChanges"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"  # noqa: E501
    }

    CSS_SELECTOR = "div.ajtzPLeO.b8dd3F0y > a"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def process_crawling(self) -> List[str]:
        href_list: list[str] = []
        self.log.info(f"--- 나무위키 수집 시작: {self.RECENT_CHANGES_URL} ---")

        try:
            async with aiohttp.ClientSession() as session:
                response = await session.get(
                    self.RECENT_CHANGES_URL, headers=self.HEADERS, timeout=10
                )

                response.raise_for_status()
                html_content = await response.text()

            soup = BeautifulSoup(html_content, "html.parser")
            elements = soup.select(self.CSS_SELECTOR)

            if not elements:
                raise AirflowSkipException(
                    f"요소를 찾을 수 없습니다. (Selector: '{self.CSS_SELECTOR}') "
                    "나무위키 프론트엔드 업데이트로 클래스명이 변경되었을 수 있습니다."
                )
                return []

            for element in elements:
                href = element.get("href")
                if href and href.startswith("/w/"):
                    full_url = f"{self.BASE_URL}{href}"
                    href_list.append(full_url)

            def clear_image_urls_fn(url: str) -> bool:
                """
                파일 링크를 제외하는 필터 함수
                :param url: target url to check
                :return: true if not a file link
                :rtype:
                """
                file_extensions = [
                    ".png",
                    ".jpg",
                    ".jpeg",
                    ".gif",
                    ".svg",
                    ".webp",
                    ".bmp",
                ]
                return not any(url.lower().endswith(ext) for ext in file_extensions)

            result = list(
                filter(
                    clear_image_urls_fn,
                    href_list,
                )
            )

            self.log.info(f"나무위키 수집 완료: {len(href_list)}개 URL")
            self.log.info(f"Cleaned URLs: {len(result)}개 (파일 링크 제외)")

            return result

        except aiohttp.ClientError as e:
            self.log.error(f"나무위키 네트워크 오류: {e}")
        except Exception as e:
            self.log.error(f"나무위키 파싱 중 오류: {e}")

        return []
