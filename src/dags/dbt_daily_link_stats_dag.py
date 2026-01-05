from datetime import datetime

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


dbt_daily_link_stats = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    dag_id="dbt_daily_link_stats",
    start_date=datetime(2025, 12, 1),
    schedule="@daily",
    catchup=False,
    tags={"summary", "stats", "marts"},
    operator_args={
        "vars": {"check_date": "{{ ds }}"},
    },
    render_config=RenderConfig(
        select=["+fct_daily_link_counts"],
    ),
)
