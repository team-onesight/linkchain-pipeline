import asyncio
import logging
from enum import Enum
from typing import List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class VelogPostType(Enum):
    RECENT = "posts"  # 최신 글
    CURATED = "curated-posts"  # 추천 글 (트렌딩 아님, 메인 피드)


class VelogTrendingTimeframe(Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class VelogCrawler:
    """Velog 게시물 수집을 위한 비동기 크롤러 클래스"""

    BASE_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"  # noqa: E501
    }

    _TRENDING_QUERY = """
    query trendingPosts($input: TrendingPostsInput!) {
        trendingPosts(input: $input) {
            id
            user {
                username
            }
            url_slug
        }
    }
    """

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    @staticmethod
    def _to_url(username: Optional[str], slug: Optional[str]) -> Optional[str]:
        if not username or not slug:
            return None
        return f"https://velog.io/@{username}/{slug}"

    async def get_feed_posts(
        self, post_type: VelogPostType = VelogPostType.RECENT, max_limit: int = 50
    ) -> List[str]:
        """
        최신(posts) 또는 추천(curated-posts) 피드 수집 (REST API 방식)
        """
        api_url = f"https://cache.velcdn.com/api/{post_type.value}"
        collected_urls: List[str] = []
        cursor: Optional[str] = None

        logger.info(
            f"--- Velog Feed [{post_type.value}] 수집 시작 (목표: {max_limit}개) ---"
        )

        while len(collected_urls) < max_limit:
            params = {"cursor": cursor} if cursor else {}

            try:
                async with self.session.get(
                    api_url, headers=self.BASE_HEADERS, params=params
                ) as response:
                    response.raise_for_status()
                    posts_data = await response.json()

                if not posts_data:
                    break

                for post in posts_data:
                    username = (post.get("user") or {}).get("username")
                    slug = post.get("urlSlug")
                    full_url = self._to_url(username, slug)

                    if full_url:
                        collected_urls.append(full_url)

                if len(collected_urls) >= max_limit:
                    break

                cursor = posts_data[-1].get("id")
                if not cursor:
                    break

                logger.info(f"현재 {len(collected_urls)}개 수집 완료...")
                await asyncio.sleep(0.5)  # 부하 방지

            except Exception as e:
                logger.error(f"Feed 수집 중 에러 발생: {e}")
                break

        return collected_urls[:max_limit]

    async def get_trending_posts(
        self,
        timeframe: VelogTrendingTimeframe = VelogTrendingTimeframe.DAY,
        max_limit: int = 100,
    ) -> List[str]:
        """
        트렌딩 게시물 수집 (GraphQL API 방식)
        """
        api_url = "https://v3.velog.io/graphql"
        collected_urls: List[str] = []
        offset = 0
        limit = 20

        logger.info(
            f"--- Velog Trending [{timeframe.value}] 수집 시작 (목표: {max_limit}개) ---"
        )

        while len(collected_urls) < max_limit:
            payload = {
                "query": self._TRENDING_QUERY,
                "variables": {
                    "input": {
                        "limit": limit,
                        "offset": offset,
                        "timeframe": timeframe.value,
                    }
                },
            }

            try:
                async with self.session.post(
                    api_url, headers=self.BASE_HEADERS, json=payload
                ) as response:
                    response.raise_for_status()
                    result = await response.json()

                posts_data = result.get("data", {}).get("trendingPosts", [])

                if not posts_data:
                    break

                for post in posts_data:
                    username = (post.get("user") or {}).get("username")
                    slug = post.get("url_slug")
                    full_url = self._to_url(username, slug)

                    if full_url:
                        collected_urls.append(full_url)

                offset += limit

                logger.info(f"현재 {len(collected_urls)}개 수집 완료...")
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Trending 수집 중 에러 발생: {e}")
                break

        return collected_urls[:max_limit]
