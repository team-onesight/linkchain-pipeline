from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from operators.fetch_and_save_html_operator import FetchAndSaveHtmlOperator

DBT_PROJECT_PATH = "/opt/airflow/dags/dbt/linkchain"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)
profile_config = ProfileConfig(
    profile_name="linkchain",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id="snowflake_default"),
)
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    execution_mode=ExecutionMode.LOCAL,
)
with DAG(
    dag_id="fetch_html_and_save_dag",
    schedule="*/20 * * * *",
    doc_md="Fetches HTML, saves to S3, logs metadata using Custom Operator.",
    start_date=None,
    catchup=False,
    tags={"crawling", "html", "raw_data"},
) as dag:
    start = EmptyOperator(task_id="start")

    create_view_link_need_to_be_fetched = DbtTaskGroup(
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        group_id="dbt_create_view_link_need_to_be_fetched",
        render_config=RenderConfig(
            select=["+link_need_to_be_fetched"],
        ),
    )

    fetch_html_task = FetchAndSaveHtmlOperator(
        task_id="fetch_html_and_save",
        snowflake_conn_id="snowflake_default",
        aws_conn_id="aws_default",
        limit=100,
        s3_bucket_name="de7-team1",
        pool="html_fetcher_pool",
    )

    end = EmptyOperator(task_id="end")

    start >> create_view_link_need_to_be_fetched >> fetch_html_task >> end
