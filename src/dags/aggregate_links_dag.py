import json
import logging

import polars as pl
from aggregator.processor.factory import ProcessorFactory
from airflow.exceptions import (
    AirflowFailException,
    AirflowSkipException,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from hooks.snowflake_aggregation_hook import SnowflakeAggregationHook

logger = logging.getLogger(__name__)


def _fetch_active_rules():
    from hooks.snowflake_aggregation_hook import SnowflakeAggregationHook

    hook = SnowflakeAggregationHook(database="LINKCHAIN_ANALYTICS", schema="ANALYTICS")
    df = hook.get_active_rules()
    mapped_tasks_input = []

    for row in df.iter_rows(named=True):
        raw_params = row["rule_params"]
        parsed_params = (
            json.loads(raw_params) if isinstance(raw_params, str) else raw_params
        )

        rule_data = {
            "group_code": row["group_code"],
            "group_title": row["group_title"],
            "rule_type": row["rule_type"],
            "rule_params": parsed_params,
        }

        mapped_tasks_input.append({"rule_row": rule_data})

    return mapped_tasks_input


def _execute_group_processing(rule_row: dict):
    """
    Dynamic Task Mapping에 의해 병렬로 실행되는 실제 작업 단위
    rule_row에는 하나의 그룹 규칙 정보가 들어있음
    """
    group_code = rule_row["group_code"]
    group_title = rule_row["group_title"]
    rule_type = rule_row["rule_type"]
    rule_params = rule_row["rule_params"]

    logger.info(f"processing group info: {group_code} ({group_title})")
    logger.info(f"rule: {rule_type} / params: {rule_params}")

    hook = SnowflakeAggregationHook(database="LINKCHAIN_ANALYTICS", schema="ANALYTICS")
    df_raw = hook.get_link_clustered()

    if df_raw.is_empty():
        raise AirflowSkipException("there is no data to process.")
    try:
        strategy = ProcessorFactory.get_strategy(rule_type)
        df_filtered = strategy.process(df_raw, rule_params)

        if df_filtered.height > 0:
            final_df = df_filtered.select(
                pl.col("link_id"),
                pl.lit(group_title).alias("group_title"),
                pl.lit(group_code).alias("group_code"),
            )

            row_count = final_df.height
            logger.info(f"{row_count} rows matched for group {group_code}")

            # --- [TODO: DB 적재 로직] ---

            logger.info(final_df.head())

        else:
            raise AirflowSkipException("no rows matched the criteria.")
    except AirflowSkipException as e:
        logger.info(f"Skipping group {group_code}: {e}")
        raise AirflowSkipException("Skipped due to no matching rows.") from e

    except Exception as e:
        logger.error(f"Error processing group {group_code}: {e}")
        raise AirflowFailException(f"Failed to process group {group_code}") from e


with DAG(
    dag_id="aggregate_links_dag",
    start_date=None,
    schedule="@daily",
    catchup=False,
    tags={"data_engineering", "dynamic_mapping"},
) as dag:
    fetch_active_rules = PythonOperator(
        task_id="fetch_active_rules",
        python_callable=_fetch_active_rules,
    )

    process_group_task = PythonOperator.partial(
        task_id="execute_group_processing",
        python_callable=_execute_group_processing,
        map_index_template="{{ task.op_kwargs['rule_row']['group_code'] }}",
        max_active_tis_per_dag=5,
    ).expand(op_kwargs=fetch_active_rules.output)

    fetch_active_rules >> process_group_task
