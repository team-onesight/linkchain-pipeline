from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable
from airflow.utils.task_group import TaskGroup
from operators.create_mapping_operator import CreateMappingOperator
from operators.olap_to_staging_operator import OlapToStagingOperator
from operators.staging_to_target_operator import StagingToTargetOperator
from sql.olap_to_oltp_link import MERGE_LINK_SQL
from sql.olap_to_oltp_link_group import LINK_GROUP_MAPPING_SQL, UPSERT_LINK_GROUP_SQL
from sql.olap_to_oltp_tag import TAG_MAPPING_SQL, UPSERT_TAG_SQL

STAGING_COLUMNS_MAPPING = {
    "link": "LINK_STAGING_COLUMNS",
    "link_group": "LINK_GROUP_STAGING_COLUMNS",
    "tag": "TAG_STAGING_COLUMNS",
}


def olap_to_oltp_task_group(
    group_id,
    olap_table,
    staging_table,
    upsert_sql,
    mapping_sql,
):
    with TaskGroup(group_id=group_id) as tg:
        variable_key = STAGING_COLUMNS_MAPPING.get(group_id.lower())
        if not variable_key:
            raise ValueError(f"Invalid group_id: {group_id}")
        staging_columns = Variable.get(variable_key, deserialize_json=True)

        olap_to_staging = OlapToStagingOperator(
            task_id="olap_to_staging",
            olap_table=olap_table,
            staging_table=staging_table,
            staging_columns=staging_columns,
        )
        staging_to_target = StagingToTargetOperator(
            task_id="staging_to_target", upsert_sql=upsert_sql
        )

        if mapping_sql:
            create_mapping = CreateMappingOperator(
                task_id="create_mapping",
                mapping_sql=mapping_sql,
            )
            olap_to_staging >> staging_to_target >> create_mapping
        else:
            olap_to_staging >> staging_to_target

    return tg


with DAG(
    dag_id="olap_to_oltp_all",
    start_date=datetime(2025, 12, 24),
    schedule="0 * * * *",
    catchup=False,
    tags=["olap", "oltp"],
) as dag:

    # link
    link_task_group = olap_to_oltp_task_group(
        group_id="link",
        olap_table="ANALYTICS.LINK_CLUSTERED",
        staging_table="staging.link",
        upsert_sql=MERGE_LINK_SQL,
        mapping_sql=None,  # link는 mapping이 없음
    )

    # link_group
    link_group_task_group = olap_to_oltp_task_group(
        group_id="link_group",
        olap_table="ANALYTICS.LINK_GROUP",
        staging_table="staging.link_group",
        upsert_sql=UPSERT_LINK_GROUP_SQL,
        mapping_sql=LINK_GROUP_MAPPING_SQL,
    )

    # tag
    tag_task_group = olap_to_oltp_task_group(
        group_id="tag",
        olap_table="ANALYTICS.TAG",
        staging_table="staging.tag",
        upsert_sql=UPSERT_TAG_SQL,
        mapping_sql=TAG_MAPPING_SQL,
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> [link_task_group, link_group_task_group, tag_task_group] >> end
