from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup
from operators.create_mapping_operator import CreateMappingOperator
from operators.olap_to_staging_operator import OlapToStagingOperator
from operators.staging_to_target_operator import StagingToTargetOperator
from sql.olap_to_oltp_link import LINK_STAGING_COLUMNS, MERGE_LINK_SQL
from sql.olap_to_oltp_link_group import (
    LINK_GROUP_MAPPING_SQL,
    LINK_GROUP_STAGING_COLUMNS,
    UPSERT_LINK_GROUP_SQL,
)
from sql.olap_to_oltp_tag import TAG_MAPPING_SQL, TAG_STAGING_COLUMNS, UPSERT_TAG_SQL


def entity_task_group(
    group_id: str,
    olap_table: str,
    staging_table: str,
    staging_columns: list[str],
    upsert_sql: str,
) -> TaskGroup:
    """
    OLAP → staging → OLTP(entity) 적재 TaskGroup
    FK 의존성이 없는 부모 테이블용
    """
    with TaskGroup(group_id=group_id) as tg:
        olap_to_staging = OlapToStagingOperator(
            task_id="olap_to_staging",
            olap_table=olap_table,
            staging_table=staging_table,
            staging_columns=staging_columns,
        )

        staging_to_target = StagingToTargetOperator(
            task_id="staging_to_target",
            upsert_sql=upsert_sql,
        )

        olap_to_staging >> staging_to_target

    return tg


def mapping_task_group(
    group_id: str,
    mapping_sql: str,
) -> TaskGroup:
    """
    FK를 가지는 mapping 테이블 전용 TaskGroup
    반드시 부모 entity 적재 이후 실행되어야 함
    """
    with TaskGroup(group_id=group_id) as tg:
        CreateMappingOperator(
            task_id="create_mapping",
            mapping_sql=mapping_sql,
        )

    return tg


with DAG(
    dag_id="olap_to_oltp_dag",
    start_date=datetime(2025, 12, 24),
    schedule="0 * * * *",
    catchup=False,
    tags=["olap", "oltp"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    link_to_tg_done = EmptyOperator(task_id="link_to_tg_done")
    before_mapping = EmptyOperator(task_id="before_mapping")

    link_tg = entity_task_group(
        group_id="link",
        olap_table="ANALYTICS.LINK_CLUSTERED",
        staging_table="staging.link",
        staging_columns=LINK_STAGING_COLUMNS,
        upsert_sql=MERGE_LINK_SQL,
    )

    tag_tg = entity_task_group(
        group_id="tag",
        olap_table="ANALYTICS.TAG",
        staging_table="staging.tag",
        staging_columns=TAG_STAGING_COLUMNS,
        upsert_sql=UPSERT_TAG_SQL,
    )

    link_group_tg = entity_task_group(
        group_id="link_group",
        olap_table="ANALYTICS.LINK_GROUP",
        staging_table="staging.link_group",
        staging_columns=LINK_GROUP_STAGING_COLUMNS,
        upsert_sql=UPSERT_LINK_GROUP_SQL,
    )

    tag_mapping_tg = mapping_task_group(
        group_id="tag_mapping",
        mapping_sql=TAG_MAPPING_SQL,
    )

    link_group_mapping_tg = mapping_task_group(
        group_id="link_group_mapping",
        mapping_sql=LINK_GROUP_MAPPING_SQL,
    )

    start >> link_tg >> link_to_tg_done
    link_to_tg_done >> [tag_tg, link_group_tg]
    [tag_tg, link_group_tg] >> before_mapping
    before_mapping >> [tag_mapping_tg, link_group_mapping_tg] >> end