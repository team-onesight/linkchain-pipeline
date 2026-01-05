from cosmos import (
    DbtDag,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DBT_PROJECT_PATH = "/opt/airflow/dags/dbt/linkchain"

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)
profile_config = ProfileConfig(
    profile_name="linkchain",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id="snowflake_default"),
)
execution_config = ExecutionConfig(
    dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
    execution_mode=ExecutionMode.LOCAL,
)


dbt_create_links_need_to_be_fetched = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    dag_id="dbt_create_links_need_to_be_fetched_dag",
    start_date=None,
    schedule="@once",
    catchup=False,
    tags={"sample", "dbt"},
    render_config=RenderConfig(
        select=["+link_need_to_be_fetched"],
    ),
)
