import asyncio
from typing import List

import scrapetube
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Variable
from crawling.crawlers.abc.base_crawler import BaseCrawler


class YoutubeCrawler(BaseCrawler):
    channels = None
    max_limit = 20
    channels_variable_key = None

    def __init__(
        self,
        channels_variable_key: str = "youtube_target_channels",
        max_limit: int = 20,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.channels_variable_key = channels_variable_key
        self.max_limit = max_limit

    async def process_crawling(self) -> List[str]:
        try:
            self.channels = Variable.get(
                self.channels_variable_key, deserialize_json=True
            )
        except Exception as e:
            self.log.error(f"Variable '{self.channels_variable_key}' 로드 실패: {e}")
            raise AirflowSkipException("YoutubeCrawler 스킵 - 채널 정보 없음")  # noqa: B904

        loop = asyncio.get_running_loop()

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
