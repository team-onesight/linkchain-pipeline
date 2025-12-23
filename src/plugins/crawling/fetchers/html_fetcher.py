import asyncio
import logging
from enum import Enum
from typing import Any, Dict, List, Tuple

import aiohttp
from aiohttp import ClientTimeout
from hooks.s3_hook import S3Hook


class ErrorTypes(Enum):
    """
    에러 유형 Enum
    1. HTTP_ERROR: HTTP 상태 코드가 200이 아님
    2. EMPTY: 크롤링한 콘텐츠가 비어있음 (size: 0)
    3. CONTENT_NONE: 업로드 전 콘텐츠가 None임
    """

    HTTP_ERROR = "HTTP_ERROR"
    EMPTY = "EMPTY"
    UPLOAD_FAIL = "UPLOAD_FAIL"
    UNKNOWN = "UNKNOWN"


class AsyncHtmlToS3Fetcher:
    logger = logging.getLogger(__name__)

    def __init__(self, s3_hook: S3Hook, execution_date, max_concurrent: int = 5):
        self.s3_hook = s3_hook
        self.max_concurrent = max_concurrent
        self.execution_date = execution_date
        self.timeout = ClientTimeout(total=60, connect=10)
        self.headers = {"User-Agent": "Mozilla/5.0..."}

    @staticmethod
    def _split_results_by_status(
        results: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        결과를 성공 및 실패로 분리
        :param results: 크롤링 및 업로드 결과 리스트
        :return: 성공 및 실패 결과 리스트
        :rtype:
        """
        success_rows = []
        failure_rows = []

        for res in results:
            if res["status"] == "success":
                success_rows.append(
                    {
                        "link_id": res["link_id"],
                        "s3_path": res["s3_path"],
                        "file_size": res["file_size"],
                    }
                )
            else:
                failure_rows.append(
                    {
                        "link_id": res["link_id"],
                        "error_type": res["error_type"],
                        "error_message": res["error_message"],
                    }
                )

        return success_rows, failure_rows

    def process_fetch(
        self, target_link_list: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        HTML 크롤링 및 S3 업로드 처리
        :param target_link_list: 크롤링할 링크 정보 리스트 {link_id: str, url: str}
        :return: 성공 및 실패 결과 리스트 (success_rows, failure_rows)
        :rtype:
        """
        if not target_link_list:
            logging.info("No data to process.")
            return [], []

        logging.info(f"Starting async fetch for {len(target_link_list)} links...")

        raw_results = asyncio.run(self._run_async_crawl_html(target_link_list))

        success_rows, failure_rows = self._split_results_by_status(raw_results)

        logging.info(
            f"Batch Finished. Success: {len(success_rows)}, Failed: {len(failure_rows)}"
        )

        return self._split_results_by_status(raw_results)

    async def _run_async_crawl_html(
        self, links: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Async Wrapper for crawling and uploading HTML to S3
        set max_concurrent sessions, semaphore
        :param links: 크롤링할 링크 정보 리스트 {link_id: str, url: str}
        :return: 크롤링 및 업로드 결과 리스트
        :rtype: List[Dict[str, Any]] -> need to split by success/failure
        later with column result["status"]
        """

        connector = aiohttp.TCPConnector(limit=self.max_concurrent, ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            sem = asyncio.Semaphore(self.max_concurrent)

            crawl_link_tasks = [
                self._crawl_html_and_upload_to_S3(session, sem, link) for link in links
            ]
            return await asyncio.gather(*crawl_link_tasks)

    async def _crawl_html_and_upload_to_S3(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        link_data: Dict[str, Any],
    ) -> dict[str, Any]:
        """
        HTML 크롤링 후 S3 업로드 비동기 처리
        :param session: 비동기 HTTP 세션
        :param sem: 최대 동시 요청 수 제한 Semaphore
        :param link_data: 크롤링할 링크 정보 딕셔너리 {link_id: str, url: str}
        """

        link_id = link_data.get("link_id")
        url = link_data.get("url")
        s3_key = f"raw_data/links/{self.execution_date}/{link_id}.html"

        result = {
            "link_id": link_id,
            "url": url,
            "s3_path": None,
            "file_size": 0,
            "status": "failed",
            "error_message": None,
            "error_type": None,
        }

        async with sem:
            try:
                async with session.get(
                    url, headers=self.headers, timeout=self.timeout
                ) as response:
                    if response.status != 200:
                        result["error_type"] = ErrorTypes.HTTP_ERROR.value
                        result["error_message"] = f"HTTP {response.status}"
                        return result

                    content = await response.read()
                    file_size = len(content)

                if file_size == 0 or content is None:
                    result["error_type"] = ErrorTypes.EMPTY.value
                    result["error_message"] = "Empty Content (size: 0)"
                    return result

                is_uploaded = await asyncio.to_thread(
                    self._upload_bytes_sync, content, s3_key
                )

                if is_uploaded:
                    result["s3_path"] = s3_key
                    result["file_size"] = file_size
                    result["status"] = "success"
                else:
                    result["error_type"] = ErrorTypes.UPLOAD_FAIL.value
                    result["error_message"] = "S3 Upload Failed"

            except Exception as e:
                logging.error(f"[{link_id}] Error: {e}")
                result["error_type"] = ErrorTypes.UNKNOWN.value
                result["error_message"] = str(e)

            return result

    def _upload_bytes_sync(self, data: bytes, key: str) -> bool:
        try:
            self.s3_hook.upload_bytes(
                bytes_data=data,
                key=key,
                replace=True,
            )
            return True
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            return False
