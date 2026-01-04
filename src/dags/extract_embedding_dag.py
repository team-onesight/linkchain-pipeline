import json
import logging
import time
import pandas as pd
import os

from airflow.sdk import DAG, task
from hooks.s3_hook import S3Hook
from hooks.snowflake_analytics_hook import SnowflakeAnalyticsQueryHook
from hooks.snowflake_command_hook import SnowflakeCommandHook
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

@task
def extract_embedding(**context):
    """임베딩이 없는 url 가져오기"""
    snowflake_hook = SnowflakeAnalyticsQueryHook()
    urls_without_embedding = snowflake_hook.get_urls_without_embeddings()
    
    logger.info(
        f'Extracting {len(urls_without_embedding)} data\'s embedding'
    )

    model_path = "/opt/airflow/model"
    model_name = "paraphrase-multilingual-mpnet-base-v2"

    if not os.path.exists(model_path) or not os.listdir(model_path):
        print(f"Model not found in {model_path}, downloading")
        model = SentenceTransformer(model_name)
        model.save(model_path)

    embedding_model = SentenceTransformer(model_path)
    
    input_strings = [
        f'{title} {description} {' '.join(tags)}'
        for _, title, description, tags in urls_without_embedding
    ]
    
    embeddings = embedding_model.encode(input_strings)
    
    embedding_with_link_id = [
        (embedding.tolist(), row[0])
        for row, embedding in zip(urls_without_embedding, embeddings)
    ]
    
    s3hook = S3Hook(bucket_name='de7-team1')
    ds_nodash = context['ds_nodash']
    bytes_data = json.dumps(embedding_with_link_id).encode('utf-8')
    tmp_key_path = f'tmp/link_id_with_embedding_{ds_nodash}.json'

    s3hook.upload_bytes(
        bytes_data=bytes_data,
        key=tmp_key_path,
        replace=True
    )

    logger.info(
        f'{len(embedding_with_link_id)} links with embedding prepared'
    )
    logger.info(f'tmp file saved on: {tmp_key_path}')
    return tmp_key_path

@task
def save_embeddings(**context):
    """임베딩을 테이블에 적재"""
    s3hook = S3Hook(bucket_name='de7-team1')
    ti = context['ti']
    tmp_key_path = ti.xcom_pull(task_ids='extract_embedding')
    json_string = s3hook.download_bytes(tmp_key_path)
    link_id_with_embedding = json.loads(json_string)

    sql = """
        UPDATE LINKCHAIN.ANALYTICS.INTEGRATED_TABLE
        SET
            LINK_EMBEDDING = %s
        WHERE
            LINK_ID = %s
    """
    hook = SnowflakeCommandHook()
    start = time.time()
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, link_id_with_embedding)
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"Successfully updated {updated_count} rows.")
            logger.info(f'Execute time : {time.time() - start:.2f}s')
            return updated_count


@task
def load_final_table(**context):
    snowflake_hook = SnowflakeCommandHook()
    result = snowflake_hook.merge_integrated_table_to_final_table()
    logger.info(f'{result} rows merged to final table')

with DAG(
    dag_id="extract_embedding_dag",
    schedule="@daily",
    start_date=None,
    catchup=False,
) as dag:
    extract_embedding() >> save_embeddings() >> load_final_table()
