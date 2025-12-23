import logging

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable
from crawling.fetchers.html_fetcher import AsyncHtmlToS3Fetcher
from hooks.s3_hook import S3Hook
from hooks.snowflake_command_hook import SnowflakeCommandHook
from hooks.snowflake_raw_data_hook import SnowflakeRawDataQueryHook

logging = logging.getLogger(__name__)


def _fetch_html_and_save(
    snowflake_conn_id: str, aws_conn_id: str, limit: int, **kwargs
):
    """
    Fetches links, saves them to a temporary local directory, uploads to S3,
    and logs metadata to Snowflake. The temporary directory path is passed
    via XCom to a cleanup task.
    """
    logging.info(f"Connecting to Snowflake to fetch links... (Limit: {limit})")
    query_hook = SnowflakeRawDataQueryHook(snowflake_conn_id=snowflake_conn_id)
    records = query_hook.get_links_need_to_be_fetched(limit)

    if not records:
        raise AirflowSkipException("No links need to be fetched found")
    links_to_fetch = [{"link_id": row[0], "url": row[1]} for row in records]
    logging.info(f"Found {len(links_to_fetch)} links to fetch.")

    bucket_name = Variable.get("s3_bucket_name", default="de7-team1")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id, bucket_name=bucket_name)
    fetcher = AsyncHtmlToS3Fetcher(
        s3_hook=s3_hook, max_concurrent=10, execution_date=kwargs["ds"]
    )

    success_rows, failure_rows = fetcher.process_fetch(links_to_fetch)
    logging.info("Logging results to Snowflake...")

    command_hook = SnowflakeCommandHook(snowflake_conn_id=snowflake_conn_id)
    conn = command_hook.get_conn()
    cursor = conn.cursor()

    database = "LINKCHAIN"
    schema = "RAW_DATA"

    try:
        if success_rows:
            success_sql = f"""
                INSERT INTO {database}.{schema}.crawled_html_metadata
                (link_id, s3_path, file_size)
                VALUES (%(link_id)s, %(s3_path)s, %(file_size)s)
                """  # noqa: S608
            cursor.executemany(success_sql, success_rows)
            logging.info(
                f"Inserted {len(success_rows)} rows into crawled_html_metadata"
            )

        if failure_rows:
            fail_sql = f"""
                INSERT INTO {database}.{schema}.crawl_failure_log
                (link_id, error_message, error_type)
                VALUES (%(link_id)s, %(error_message)s, %(error_type)s)
                """  # noqa: S608
            cursor.executemany(fail_sql, failure_rows)
            logging.info(f"Inserted {len(failure_rows)} rows into crawl_failure_log")

        conn.commit()
        logging.info("Successfully committed results to Snowflake.")

    except Exception as e:
        conn.rollback()
        logging.error(f"DB Insert Failed: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

    logging.info(
        f"Task finished. Success: {len(success_rows)}, Failed: {len(failure_rows)}"
    )


with DAG(
    dag_id="fetch_html_dag",
    schedule="@hourly",
    doc_md="Fetches HTML, saves to S3, logs metadata, and cleans up temporary files.",
    start_date=None,
    catchup=False,
    tags={"crawling", "html"},
) as dag:
    start = EmptyOperator(task_id="start")

    fetch_html_and_save = PythonOperator(
        task_id="fetch_html_and_save",
        python_callable=_fetch_html_and_save,
        pool="html_fetcher_pool",
        op_kwargs={
            "snowflake_conn_id": "snowflake_default",
            "aws_conn_id": "aws_default",
            "limit": 100,
        },
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_html_and_save >> end
