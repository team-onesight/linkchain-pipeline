from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from operators.fetch_and_save_html_operator import FetchAndSaveHtmlOperator

with DAG(
    dag_id="fetch_html_and_save_dag",
    schedule="*/20 * * * *",
    doc_md="Fetches HTML, saves to S3, logs metadata using Custom Operator.",
    start_date=None,
    catchup=False,
    tags={"crawling", "html", "raw_data"},
) as dag:
    start = EmptyOperator(task_id="start")

    fetch_html_task = FetchAndSaveHtmlOperator(
        task_id="fetch_html_and_save",
        snowflake_conn_id="snowflake_default",
        aws_conn_id="aws_default",
        limit=100,
        s3_bucket_name="de7-team1",
        pool="html_fetcher_pool",
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_html_task >> end
