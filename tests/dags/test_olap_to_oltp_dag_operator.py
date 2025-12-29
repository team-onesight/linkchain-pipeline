import pytest
from operators.create_mapping_operator import CreateMappingOperator
from operators.olap_to_staging_operator import OlapToStagingOperator
from operators.staging_to_target_operator import StagingToTargetOperator


# fixture 생성
@pytest.fixture
def mock_context(mocker):
    """
    Airflow operator execute(context) 용 mock context
    """
    ti = mocker.MagicMock()
    return {"ti": ti}


# OlapToStagingOperator Test
def test_olap_to_staging_operator_success(mocker, mock_context):
    # given
    mock_sf_hook_cls = mocker.patch(
        "operators.olap_to_staging_operator.SnowflakeAnalyticsQueryHook"
    )
    mock_pg_hook_cls = mocker.patch(
        "operators.olap_to_staging_operator.PostgresOlapToOltpHook"
    )

    mock_sf_hook_cls.return_value.get_olap_table_data.return_value = (
        ["id", "url"],
        [(1, "a"), (2, "b")],
    )

    op = OlapToStagingOperator(
        task_id="olap_to_staging",
        olap_table="olap.link_clustered",
        staging_table="staging.link",
        staging_columns=["id", "url"],
    )

    # when
    op.execute(context=mock_context)

    # then
    mock_sf_hook_cls.return_value.get_olap_table_data.assert_called_once_with(
        table_name="olap.link_clustered",
        columns=["id", "url"],
    )
    mock_pg_hook_cls.assert_called_once_with(
        postgres_conn_id="postgres_default"
    )
    mock_pg_hook_cls.return_value.truncate_and_insert_rows.assert_called_once_with(
        table="staging.link",
        columns=["id", "url"],
        rows=[(1, "a"), (2, "b")],
    )


# StagingToTargetOperator Test
def test_staging_to_target_operator_success(mocker, mock_context):
    # given
    mock_pg_hook_cls = mocker.patch(
        "operators.staging_to_target_operator.PostgresOlapToOltpHook"
    )

    upsert_sql = "MERGE INTO public.links ..."

    op = StagingToTargetOperator(
        task_id="staging_to_target",
        upsert_sql=upsert_sql,
    )

    # when
    op.execute(context=mock_context)

    # then
    mock_pg_hook_cls.assert_called_once_with(
        postgres_conn_id="postgres_default"
    )
    mock_pg_hook_cls.return_value.upsert_table.assert_called_once_with(
        upsert_sql
    )


# CreateMappingOperator Test
def test_create_mapping_operator_success(mocker, mock_context):
    # given
    mock_pg_hook_cls = mocker.patch(
        "operators.create_mapping_operator.PostgresOlapToOltpHook"
    )

    mapping_sql = "INSERT INTO mapping_table SELECT ..."

    op = CreateMappingOperator(
        task_id="create_mapping",
        mapping_sql=mapping_sql,
    )

    # when
    op.execute(context=mock_context)

    # then
    mock_pg_hook_cls.assert_called_once_with(
        postgres_conn_id="postgres_default"
    )
    mock_pg_hook_cls.return_value.upsert_table.assert_called_once_with(
        mapping_sql
    )
