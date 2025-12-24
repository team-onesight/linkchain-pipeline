from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator
from crawling.fetchers.async_html_to_s3_fetcher import AsyncHtmlToS3Fetcher
from hooks.s3_hook import S3Hook
from hooks.snowflake_command_hook import SnowflakeCommandHook
from hooks.snowflake_raw_data_hook import SnowflakeRawDataQueryHook


class FetchAndSaveHtmlOperator(BaseOperator):
    """
    Snowflake에서 대상 URL을 조회하여 HTML을 크롤링/S3저장 후,
    결과 메타데이터를 다시 Snowflake에 저장하는 오퍼레이터
    """

    template_fields = ("limit", "s3_bucket_name")

    def __init__(
        self,
        snowflake_conn_id: str = "snowflake_default",
        aws_conn_id: str = "aws_default",
        limit: int = 100,
        s3_bucket_name: str = "de7-team1",
        database: str = "LINKCHAIN",
        schema: str = "RAW_DATA",
        max_concurrent: int = 10,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.aws_conn_id = aws_conn_id
        self.limit = limit
        self.s3_bucket_name = s3_bucket_name
        self.database = database
        self.schema = schema
        self.max_concurrent = max_concurrent

    def execute(self, context):
        execution_date = context["ds"]
        self.log.info(f"Starting Task for date: {execution_date} (Limit: {self.limit})")

        # 1. Fetch Target Links from Snowflake
        records = self._get_links_from_snowflake()

        if not records:
            self.log.info("No links to fetch. Skipping task.")
            raise AirflowSkipException("No links need to be fetched found")

        links_to_fetch = [{"link_id": row[0], "url": row[1]} for row in records]
        self.log.info(f"Found {len(links_to_fetch)} links to fetch.")

        # 2. Process Async Crawl & Upload to S3
        success_rows, failure_rows = self._process_crawling(
            links_to_fetch, execution_date
        )

        # 3. Save Results to Snowflake
        self._save_successful_results_to_snowflake(success_rows)
        self._save_failed_results_to_snowflake(failure_rows)

        total_count = len(success_rows) + len(failure_rows)
        if total_count > 0 and len(failure_rows) / total_count > 0.5:
            raise AirflowException(
                f"More than 50% of crawls failed: {len(failure_rows)}/{total_count}"
            )

    def _get_links_from_snowflake(self):
        query_hook = SnowflakeRawDataQueryHook(snowflake_conn_id=self.snowflake_conn_id)
        return query_hook.get_links_need_to_be_fetched(self.limit)

    def _process_crawling(self, links_to_fetch, execution_date):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, bucket_name=self.s3_bucket_name)
        fetcher = AsyncHtmlToS3Fetcher(
            s3_hook=s3_hook,
            max_concurrent=self.max_concurrent,
            execution_date=execution_date,
        )
        return fetcher.process(links_to_fetch)

    def _save_successful_results_to_snowflake(self, success_rows):
        command_hook = SnowflakeCommandHook(snowflake_conn_id=self.snowflake_conn_id)
        conn = command_hook.get_conn()
        cursor = conn.cursor()

        try:
            if success_rows:
                success_sql = f"""
                    INSERT INTO {self.database}.{self.schema}.crawled_html_metadata
                    (link_id, s3_path, file_size)
                    VALUES (%(link_id)s, %(s3_path)s, %(file_size)s)
                """  # noqa: S608
                cursor.executemany(success_sql, success_rows)
                self.log.info(
                    f"Inserted {len(success_rows)} rows into crawled_html_metadata"
                )

            conn.commit()
            self.log.info("Successfully committed successful results to Snowflake.")

        except Exception as e:
            conn.rollback()
            self.log.error(f"DB Insert Failed: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

    def _save_failed_results_to_snowflake(self, failure_rows):
        command_hook = SnowflakeCommandHook(snowflake_conn_id=self.snowflake_conn_id)
        conn = command_hook.get_conn()
        cursor = conn.cursor()

        try:
            if failure_rows:
                fail_sql = f"""
                    INSERT INTO {self.database}.{self.schema}.crawl_failure_log
                    (link_id, error_message, error_type)
                    VALUES (%(link_id)s, %(error_message)s, %(error_type)s)
                """  # noqa: S608
                cursor.executemany(fail_sql, failure_rows)
                self.log.info(
                    f"Inserted {len(failure_rows)} rows into crawl_failure_log"
                )

            conn.commit()
            self.log.info("Successfully committed failure results to Snowflake.")

        except Exception as e:
            conn.rollback()
            self.log.error(f"DB Insert Failed: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()
