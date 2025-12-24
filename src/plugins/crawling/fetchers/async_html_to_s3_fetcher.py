import asyncio
import logging
from enum import Enum
from typing import Any, Dict, List, Tuple

from hooks.s3_hook import S3Hook
from playwright.async_api import (
    TimeoutError as PlaywrightTimeoutError,
)
from playwright.async_api import (
    async_playwright,
)


class ErrorTypes(Enum):
    """
    에러 유형 Enum
    """

    HTTP_ERROR = "HTTP_ERROR"
    EMPTY = "EMPTY"
    UPLOAD_FAIL = "UPLOAD_FAIL"
    TIMEOUT = "TIMEOUT"
    UNKNOWN = "UNKNOWN"


class AsyncHtmlToS3Fetcher:
    """
    Playwright를 사용하여 동적 페이지를 렌더링 후 HTML을 S3에 업로드하는 Fetcher.
    """

    logger = logging.getLogger(__name__)

    BLOCKED_RESOURCE_TYPES = {
        "image",
        "media",
        "font",
        "stylesheet",
        "other",
        "websocket",
    }

    def __init__(self, s3_hook: S3Hook, execution_date, max_concurrent: int = 5):
        self.s3_hook = s3_hook
        self.max_concurrent = max_concurrent
        self.execution_date = execution_date

        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0 Safari/537.36"  # noqa: E501

    @staticmethod
    def _split_results_by_status(
        results: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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

    async def _intercept_route(self, route):
        if route.request.resource_type in self.BLOCKED_RESOURCE_TYPES:
            await route.abort()
        else:
            await route.continue_()

    def process(
        self, target_link_list: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        [Entry Point] Playwright 크롤링 및 S3 업로드 처리
        """
        if not target_link_list:
            self.logger.info("No data to process.")
            return [], []

        self.logger.info(
            f"Starting Playwright fetch for {len(target_link_list)} links..."
        )

        raw_results = asyncio.run(self._run_with_playwright(target_link_list))

        success_rows, failure_rows = self._split_results_by_status(raw_results)

        self.logger.info(
            f"Batch Finished. Success: {len(success_rows)}, Failed: {len(failure_rows)}"
        )

        return success_rows, failure_rows

    async def _run_with_playwright(
        self, links: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )

            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 1280, "height": 720},
            )

            await context.route("**/*", self._intercept_route)

            sem = asyncio.Semaphore(self.max_concurrent)

            tasks = [self._crawl_page_and_upload(context, sem, link) for link in links]
            results = await asyncio.gather(*tasks)

            await context.close()
            await browser.close()

            return results

    async def _crawl_page_and_upload(
        self,
        context,
        sem: asyncio.Semaphore,
        link_data: Dict[str, Any],
    ) -> dict[str, None | str | int | Any] | None:
        """
        개별 페이지 크롤링 -> HTML 추출 -> S3 업로드
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
            page = await context.new_page()
            try:
                response = await page.goto(
                    url, timeout=10000, wait_until="domcontentloaded"
                )

                if not response:
                    result["error_type"] = ErrorTypes.UNKNOWN.value
                    result["error_message"] = "No Response"
                    return result

                if response.status != 200:
                    result["error_type"] = ErrorTypes.HTTP_ERROR.value
                    result["error_message"] = f"HTTP {response.status}"
                    return result

                content_str = await page.content()

                content_bytes = content_str.encode("utf-8")
                file_size = len(content_bytes)

                if file_size == 0:
                    result["error_type"] = ErrorTypes.EMPTY.value
                    result["error_message"] = "Empty Content (size: 0)"
                    return result

                is_uploaded = await asyncio.to_thread(
                    self._upload_bytes_to_s3_sync, content_bytes, s3_key
                )

                if is_uploaded:
                    result["s3_path"] = s3_key
                    result["file_size"] = file_size
                    result["status"] = "success"
                else:
                    result["error_type"] = ErrorTypes.UPLOAD_FAIL.value
                    result["error_message"] = "S3 Upload Failed"

            except PlaywrightTimeoutError:
                self.logger.error(f"[{link_id}] Timeout")
                result["error_type"] = ErrorTypes.TIMEOUT.value
                result["error_message"] = "Page Load Timeout"

            except Exception as e:
                self.logger.error(f"[{link_id}] Error: {e}")
                result["error_type"] = ErrorTypes.UNKNOWN.value
                result["error_message"] = str(e)

            finally:
                await page.close()

            return result

    def _upload_bytes_to_s3_sync(self, data: bytes, key: str) -> bool:
        try:
            self.s3_hook.upload_bytes(
                bytes_data=data,
                key=key,
                replace=True,
            )
            return True
        except Exception as e:
            self.logger.error(f"Upload failed: {e}")
            return False
