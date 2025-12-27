import asyncio
from typing import List

import scrapetube
from airflow.exceptions import AirflowSkipException
from crawling.crawlers.abc.base_crawler import BaseCrawler


class YoutubeCrawler(BaseCrawler):
    channels = None
    max_limit = 20

    def __init__(
        self,
        channels: list[str],
        max_limit: int = 20,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.channels = channels
        self.max_limit = max_limit

    async def process_crawling(self) -> List[str]:
        loop = asyncio.get_running_loop()

        if not self.channels:
            raise AirflowSkipException("there is no channels to crawl.")

        futures = [
            loop.run_in_executor(
                None, self._crawl_single_channel, channel, self.max_limit
            )
            for channel in self.channels
        ]

        results = await asyncio.gather(*futures)

        return [url for channel_urls in results for url in channel_urls]

    def _crawl_single_channel(self, channel_handle: str, max_limit: int) -> List[str]:
        video_urls = []
        self.log.info(f"[{channel_handle}] 수집 시작 (Scrapetube)")

        try:
            channel_url = f"https://www.youtube.com/{channel_handle}"

            videos = scrapetube.get_channel(channel_url=channel_url, limit=max_limit)

            for video in videos:
                video_id = video["videoId"]
                full_url = f"https://www.youtube.com/watch?v={video_id}"
                video_urls.append(full_url)

                if len(video_urls) >= max_limit:
                    break

            self.log.info(f"[{channel_handle}] {len(video_urls)}개 수집 완료")

        except Exception as e:
            self.log.error(f"[{channel_handle}] 에러 발생: {e}")

        return video_urls
