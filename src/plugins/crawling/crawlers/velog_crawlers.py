import asyncio
from enum import Enum
from typing import List, Optional

import aiohttp
from crawling.crawlers.abc.base_velog_crawler import BaseVelogCrawler


class VelogPostType(Enum):
    RECENT = "posts"  # 최신 글
    CURATED = "curated-posts"  # 추천 글 (트렌딩 아님, 메인 피드)


class VelogTrendingTimeframe(Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class VelogTrendingCrawler(BaseVelogCrawler):
    """
    Velog 트렌딩 게시물 크롤러 (GraphQL API 방식)
    """

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

    def __init__(
        self,
        timeframes: list[VelogTrendingTimeframe] | None = None,
        max_limit: int = 100,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.per_max_limit = max_limit
        self.timeframes = timeframes

    async def process_crawling(self) -> List[str]:
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._get_trending_posts(
                    session=session, timeframe=tf, max_limit=self.per_max_limit
                )
                for tf in self.timeframes
            ]

            results = await asyncio.gather(*tasks)
            return [url for timeframe_urls in results for url in timeframe_urls]

    async def _get_trending_posts(
        self,
        session,
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

        self.log.info(
            f"--- Velog Trending [{timeframe.value}] 수집 시작 (목표: {max_limit}개) ---"  # noqa: E501
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
                async with session.post(
                    api_url, headers=self.HEADERS, json=payload
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

                self.log.info(f"현재 {len(collected_urls)}개 수집 완료...")
                await asyncio.sleep(0.5)

            except Exception as e:
                self.log.error(f"Trending 수집 중 에러 발생: {e}")
                break

        return collected_urls[:max_limit]


class VelogFeedCrawler(BaseVelogCrawler):
    post_types: list[VelogPostType] = [VelogPostType.RECENT]
    max_limit: int = 50

    def __init__(
        self,
        post_types: list[VelogPostType],
        max_limit: int = 50,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.post_types = post_types
        self.max_limit = max_limit

    async def process_crawling(self) -> list[str]:
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._get_feed_posts(
                    session=session, post_type=pt, max_limit=self.max_limit
                )
                for pt in self.post_types
            ]
            results = await asyncio.gather(*tasks)
            return [url for timeframe_urls in results for url in timeframe_urls]

    async def _get_feed_posts(
        self,
        session,
        post_type: VelogPostType = VelogPostType.RECENT,
        max_limit: int = 50,
    ) -> List[str]:
        """
        최신(posts) 또는 추천(curated-posts) 피드 수집 (REST API 방식)
        """
        api_url = f"https://cache.velcdn.com/api/{post_type.value}"
        collected_urls: List[str] = []
        cursor: Optional[str] = None

        self.log.info(
            f"--- Velog Feed [{post_type.value}] 수집 시작 (목표: {max_limit}개) ---"
        )

        while len(collected_urls) < max_limit:
            params = {"cursor": cursor} if cursor else {}

            try:
                async with session.get(
                    api_url, headers=self.HEADERS, params=params
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

                self.log.info(f"현재 {len(collected_urls)}개 수집 완료...")
                await asyncio.sleep(0.5)

            except Exception as e:
                self.log.error(f"Feed 수집 중 에러 발생: {e}")
                break

        return collected_urls[:max_limit]
