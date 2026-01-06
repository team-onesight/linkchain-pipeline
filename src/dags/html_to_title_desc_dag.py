import json
import logging
import time
from datetime import datetime

from airflow.sdk import DAG, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from extractor.html_extractor import extract_records_from_html
from hooks.s3_hook import S3Hook
from hooks.snowflake_command_hook import SnowflakeCommandHook

logger = logging.getLogger(__name__)

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

@task
def merge_combined_sources_to_integrated_table() -> int:
    """통합 테이블로 데이터 병합"""
    hook = SnowflakeCommandHook()
    count = hook.merge_combined_sources_to_integrated_table()
    logger.info(f'{count} links merged into INTEGRATED_TABLE')
    return count

@task
def get_urls_without_title_desc_image_url(**context) -> str:
    """Title/Desc가 NULL인 대상 목록 가져오기"""
    snowflake_hook = SnowflakeCommandHook()
    urls = snowflake_hook.get_integrated_table_where_title_desc_is_null()
    link_id_with_s3_path = [(row[0], row[1]) for row in urls]

    s3hook = S3Hook(bucket_name='de7-team1')
    ds_nodash = context['ds_nodash']
    bytes_data = json.dumps(link_id_with_s3_path).encode('utf-8')
    tmp_key_path = f'tmp/link_id_with_s3_path_{ds_nodash}.json'

    s3hook.upload_bytes(
        bytes_data=bytes_data,
        key=tmp_key_path,
        replace=True
    )

    logger.info(
        f'{len(link_id_with_s3_path)} links looking for title and description'
    )
    logger.info(f'tmp file saved on: {tmp_key_path}')
    return tmp_key_path

@task
def extract_title_desc_image_url(**context)-> str:
    """S3에서 HTML 다운로드 및 정보 추출"""
    s3hook = S3Hook(bucket_name='de7-team1')
    ti = context['ti']
    tmp_key_path = ti.xcom_pull(task_ids='get_urls_without_title_desc_image_url')
    if not tmp_key_path:
        raise ValueError(
            "XCom pull 실패: get_urls_without_title_desc_image_url 에서 tmp_key_path 없음" # noqa: E501
        )
    json_string = s3hook.download_bytes(tmp_key_path)
    link_id_with_s3_path = json.loads(json_string)

    link_id_with_title_desc_image_url = []
    start = time.time()
    logger.info(
        f'Extracting {len(link_id_with_s3_path)} data\'s title and descriptions'
    )

    for link_id, s3_path in link_id_with_s3_path:
        logger.info(f'link_id: {link_id}, s3_path: {s3_path}')
        html = s3hook.download_bytes(s3_path)
        title, description, image_url = extract_records_from_html(html)
        if title is not None:
            link_id_with_title_desc_image_url.append((title, description, image_url, link_id)) # noqa: E501

    bytes_data = json.dumps(link_id_with_title_desc_image_url).encode('utf-8')
    logger.info(f'bytes_data: {bytes_data}')
    ds_nodash = context['ds_nodash']
    tmp_key_path = f'tmp/title_desc_url_{ds_nodash}.json'

    s3hook.upload_bytes(
        bytes_data=bytes_data,
        key=tmp_key_path,
        replace=True
    )

    logger.info(f'{len(link_id_with_title_desc_image_url)} data waiting for update')
    logger.info(
        f'excluded {len(link_id_with_s3_path)-len(link_id_with_title_desc_image_url)} data' # noqa: E501
    )
    logger.info(f'execute time : {time.time() - start:.2f}s')
    logger.info(f'tmp file saved on: {tmp_key_path}')
    return tmp_key_path

@task
def update_to_integrated_table(**context) -> int:
    """최종 테이블 업데이트"""
    s3hook = S3Hook(bucket_name='de7-team1')
    ti = context['ti']
    tmp_key_path = ti.xcom_pull(task_ids='extract_title_desc_image_url')
    if not tmp_key_path:
        raise ValueError(
            "XCom pull 실패: extract_title_desc_image_url 에서 tmp_key_path 없음"
        )
    json_string = s3hook.download_bytes(tmp_key_path)
    formatted_data = json.loads(json_string)

    create_staging_sql = """
        CREATE OR REPLACE TEMP TABLE staging.integrated_table (
            LINK_ID VARCHAR(16777216) NOT NULL,
            TITLE VARCHAR(16777216),
            DESCRIPTION VARCHAR(16777216),
            IMAGE_URL VARCHAR(16777216)
        )
    """

    insert_sql = """
        INSERT INTO staging.integrated_table (LINK_ID, TITLE, DESCRIPTION, IMAGE_URL)
        VALUES (%s, %s, %s, %s)
    """

    merge_sql = """
        MERGE INTO analytics.analytics.integrated_table AS target
        USING staging.integrated_table AS source
        ON target.LINK_ID = source.LINK_ID
        WHEN MATCHED
            AND (target.TITLE IS NULL OR target.DESCRIPTION IS NULL OR target.IMAGE_URL IS NULL)
        THEN UPDATE SET
            TITLE = COALESCE(target.TITLE, source.TITLE),
            DESCRIPTION = COALESCE(target.DESCRIPTION, source.DESCRIPTION),
            IMAGE_URL = COALESCE(target.IMAGE_URL, source.IMAGE_URL)
    """

    hook = SnowflakeCommandHook()
    start = time.time()
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_staging_sql)
            cursor.executemany(insert_sql, formatted_data)
            cursor.execute(merge_sql)
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"Successfully updated {updated_count} rows.")
            logger.info(f'execute time : {time.time() - start:.2f}s')
            return updated_count



with DAG(
    dag_id="html_to_title_desc_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 4, 0, 0),
    catchup=False,
) as dag:

    create_view_link_need_to_be_fetched = DbtTaskGroup(
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        group_id="dbt_create_view_link_need_to_be_fetched",
        render_config=RenderConfig(
            select=["+combined_links"],
        ),
    )

    create_view_link_need_to_be_fetched >> \
    merge_combined_sources_to_integrated_table() >> \
    get_urls_without_title_desc_image_url() >> \
    extract_title_desc_image_url() >> \
    update_to_integrated_table()
