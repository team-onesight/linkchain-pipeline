import logging
import time
import json

import pandas as pd
from airflow.sdk import DAG, task
from extractor.html_extractor import extract_records_from_html
from hooks.s3_hook import S3Hook
from hooks.snowflake_command_hook import SnowflakeCommandHook


@task
def merge_combined_sources_to_integrated_table() -> int:
    """통합 테이블로 데이터 병합"""
    hook = SnowflakeCommandHook()
    count = hook.merge_combined_sources_to_integrated_table()
    logging.info(f'{count} links merged into INTEGRATED_TABLE')
    return count

@task
def get_urls_without_title_desc(**context) -> list:
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

    logging.info(
        f'{len(link_id_with_s3_path)} links looking for title and description'
    )
    logging.info(f'tmp file saved on: {tmp_key_path}')
    return tmp_key_path

@task
def extract_title_desc(tmp_key_path: str, **context)-> list:
    """S3에서 HTML 다운로드 및 정보 추출"""
    s3hook = S3Hook(bucket_name='de7-team1')
    json_string = s3hook.download_bytes(tmp_key_path)
    link_id_with_s3_path = json.loads(json_string)

    link_id_with_title_desc = []
    start = time.time()
    logging.info(
        f'Extracting {len(link_id_with_s3_path)} data\'s title and descriptions'
    )

    for link_id, s3_path in link_id_with_s3_path:
        html = s3hook.download_bytes(s3_path)
        title, description = extract_records_from_html(html)
        if title is not None:
            link_id_with_title_desc.append((title, description, link_id))

    bytes_data = json.dumps(link_id_with_title_desc).encode('utf-8')
    ds_nodash = context['ds_nodash']
    tmp_key_path = f'tmp/title_desc_url_{ds_nodash}.json'

    s3hook.upload_bytes(
        bytes_data=bytes_data,
        key=tmp_key_path,
        replace=True
    )

    logging.info(f'{len(link_id_with_title_desc)} data waiting for update')
    logging.info(
        f'excluded {len(link_id_with_s3_path)-len(link_id_with_title_desc)} data'
    )
    logging.info(f'execute time : {time.time() - start:.2f}s')
    logging.info(f'tmp file saved on: {tmp_key_path}')
    return tmp_key_path

@task
def update_to_integrated_table(tmp_key_path: str) -> int:
    """최종 테이블 업데이트"""
    s3hook = S3Hook(bucket_name='de7-team1')
    json_string = s3hook.download_bytes(tmp_key_path)
    formatted_data = json.loads(json_string)

    sql = """
        UPDATE LINKCHAIN.ANALYTICS.INTEGRATED_TABLE
        SET
            TITLE = %s,
            DESCRIPTION = %s
        WHERE
            LINK_ID = %s
    """
    hook = SnowflakeCommandHook()
    start = time.time()
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, formatted_data)
            updated_count = cursor.rowcount
            conn.commit()
            logging.info(f"Successfully updated {updated_count} rows.")
            logging.info(f'execute time : {time.time() - start:.2f}s')
            return updated_count



with DAG(
    dag_id="html_to_title_desc_dag",
    schedule="@daily",
    start_date=None,
    catchup=False,
) as dag:
    merged_links = merge_combined_sources_to_integrated_table()


    link_id_with_s3_path = get_urls_without_title_desc()


    formatted_data = extract_title_desc(link_id_with_s3_path)


    update_count = update_to_integrated_table(formatted_data)


    merged_links >> link_id_with_s3_path >> formatted_data >> update_count
