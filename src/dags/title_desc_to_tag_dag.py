import json
import logging
import time
import pandas as pd

from airflow.sdk import DAG, task
from extractor.tag_extractor import generate_multilang_tags
from hooks.s3_hook import S3Hook
from hooks.snowflake_analytics_hook import SnowflakeAnalyticsQueryHook
from hooks.snowflake_command_hook import SnowflakeCommandHook

logger = logging.getLogger(__name__)

@task
def get_urls_without_tags(**context) -> str:
    """Tag가 없는 대상 목록 가져오기"""
    snowflake_hook = SnowflakeAnalyticsQueryHook()
    urls = snowflake_hook.get_urls_without_tags()
    link_id_without_tags = [(row[0]) for row in urls]

    s3hook = S3Hook(bucket_name='de7-team1')
    ds_nodash = context['ds_nodash']
    bytes_data = json.dumps(link_id_without_tags).encode('utf-8')
    tmp_key_path = f'tmp/link_id_without_tags_{ds_nodash}.json'

    s3hook.upload_bytes(
        bytes_data=bytes_data,
        key=tmp_key_path,
        replace=True
    )

    logger.info(
        f'{len(link_id_without_tags)} links looking for tags'
    )
    logger.info(f'tmp file saved on: {tmp_key_path}')
    return tmp_key_path

@task
def extract_tags(**context):
    """Tag 추출"""
    s3hook = S3Hook(bucket_name='de7-team1')
    ti = context['ti']
    tmp_key_path = ti.xcom_pull(task_ids='get_urls_without_tags')
    json_string = s3hook.download_bytes(tmp_key_path)
    link_id_without_tags = json.loads(json_string)

    title_desc_df = pd.DataFrame(link_id_without_tags, columns=['link_id', 'title', 'description'])

    df = generate_multilang_tags(title_desc_df)
    link_id_with_tags = df[['link_id', 'tags']]

    s3hook = S3Hook(bucket_name='de7-team1')
    ds_nodash = context['ds_nodash']
    bytes_data = json.dumps(link_id_with_tags).encode('utf-8')
    tmp_key_path = f'tmp/link_id_with_tags_{ds_nodash}.json'

    s3hook.upload_bytes(
        bytes_data=bytes_data,
        key=tmp_key_path,
        replace=True
    )
    logger.info(
        f'{len(link_id_with_tags)} links waiting for update'
    )
    logger.info(f'tmp file saved on: {tmp_key_path}')
    return tmp_key_path

@task
def save_tags(**context):
    """최종 테이블 업데이트"""
    s3hook = S3Hook(bucket_name='de7-team1')
    ti = context['ti']
    tmp_key_path = ti.xcom_pull(task_ids='extract_tags')
    json_string = s3hook.download_bytes(tmp_key_path)
    link_id_with_tags = json.loads(json_string)

    exploded_df_list = pd.DataFrame(
        link_id_with_tags,
        columns=['link_id', 'tags']
    ).explode('tags').values.tolist()

    sql = """
        INSERT INTO LINKCHAIN.ANALYTICS.TAG(TAG_NAME, LINK_ID)
        VALUES (%s, %s)
    """
    hook = SnowflakeCommandHook()
    start = time.time()
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, exploded_df_list)
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"Successfully updated {updated_count} rows.")
            logger.info(f'execute time : {time.time() - start:.2f}s')
            return updated_count

with DAG(
    dag_id="html_to_title_desc_dag",
    schedule="@daily",
    start_date=None,
    catchup=False,
) as dag:
    get_urls_without_tags() >> extract_tags() >> save_tags()