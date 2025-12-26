"""
### OLTP to OLAP 파이프라인
이 DAG은 PostgreSQL의 운영 데이터를 Snowflake ODS 영역으로 적재하고,
Link 테이블의 url 필드에 대해 추가 파싱 작업을 수행합니다.

주요 단계:
1. PostgresToSnowflake: 운영 DB의 각 테이블을 Snowflake로 Full Refresh 적재
2. ParseLinkDetail: Link 테이블의 URL을 분석하여 URL 관련 상세 정보(domain,path,etc..) 추출 및 적재

설정 방식:
- Airflow Variable `oltp_to_olap`에서 테이블 목록과 Chunk Size를 관리합니다.
"""  # noqa: E501

from airflow.sdk import DAG, Variable
from operators.parsing_link_detail_operator import ParseLinkDetailOperator
from operators.postgres_to_snowflake_operator import PostgresToSnowflakeOperator


def get_oltp_config():
    return Variable.get("oltp_to_olap", deserialize_json=True)

config = get_oltp_config()

with DAG(
    dag_id="oltp_to_olap_dag",
    start_date=None,
    catchup=False,
    schedule="@hourly",
) as dag:
    tasks = {}
    for table_key, table_config in config["table_config"].items():
        task = PostgresToSnowflakeOperator(
            task_id=f"transfer_{table_key}",
            postgres_conn_id="postgres_conn",
            snowflake_conn_id="snowflake_conn",
            snowflake_db="linkchain",
            snowflake_schema="ods",
            table_key=table_key,
            table_config=table_config,
            chunk_size=config["chunk_size"],
            pool="oltp_to_olap_pool"
        )
        tasks[table_key] = task

    link_config = config["table_config"]["link"]

    link_detail_dest_table = "raw_data.link_detail"
    link_detail_dest_columns = [
        'link_id', 'subdomain', 'host', 'path', 'parameters', 'fragment'
        ]

    parse_link_detail = ParseLinkDetailOperator(
        task_id='parse_link_detail',
        snowflake_db = "linkchain",
        source_table=link_config["snowflake_table"],
        source_columns=["link_id", "url"],
        dest_table=link_detail_dest_table,
        dest_columns=link_detail_dest_columns,
        chunk_size=config["chunk_size"]
    )

    # task list내 모든 task 수행 + link task 이후에는 parse_link_detail 진행
    if 'link' in tasks:
        tasks['link'] >> parse_link_detail
