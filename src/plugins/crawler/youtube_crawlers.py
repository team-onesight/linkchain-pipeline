import asyncio
import logging
from typing import List
import scrapetube

logger = logging.getLogger(__name__)


def _crawl_single_channel(channel_handle: str, max_limit: int) -> List[str]:
    video_urls = []
    logger.info(f"[{channel_handle}] 수집 시작 (Scrapetube)")

    try:
        channel_url = f"https://www.youtube.com/{channel_handle}"

        videos = scrapetube.get_channel(channel_url=channel_url, limit=max_limit)

        for video in videos:
            video_id = video["videoId"]
            full_url = f"https://www.youtube.com/watch?v={video_id}"
            video_urls.append(full_url)

            if len(video_urls) >= max_limit:
                break

        logger.info(f"[{channel_handle}] {len(video_urls)}개 수집 완료")

    except Exception as e:
        logger.error(f"[{channel_handle}] 에러 발생: {e}")

    return video_urls


async def crawl_channels(channels: List[str], max_limit: int = 20) -> List[str]:
    loop = asyncio.get_running_loop()

    futures = [
        loop.run_in_executor(None, _crawl_single_channel, channel, max_limit)
        for channel in channels
    ]

    results = await asyncio.gather(*futures)

    return [url for channel_urls in results for url in channel_urls]
