"""
### OLTP to OLAP 파이프라인
이 DAG은 PostgreSQL의 운영 데이터를 Snowflake ODS 영역으로 적재하고,
Link 테이블의 url 필드에 대해 추가 파싱 작업을 수행합니다.

주요 단계:
1. PostgresToSnowflake: 운영 DB의 각 테이블을 Snowflake로 Full Refresh 적재
2. ParseLinkDetail: Link 테이블의 URL을 분석하여 URL 관련 상세 정보(domain,path,etc..) 추출 및 적재

설정 방식:
- Airflow Variable `oltp_to_olap`에서 Chunk Size를 관리합니다.
"""  # noqa: E501

from airflow.sdk import DAG
from operators.parsing_link_detail_operator import ParseLinkDetailOperator
from operators.postgres_to_snowflake_operator import PostgresToSnowflakeOperator

OLTP_CONFIG = {
    "table_config": {
        "link": {
            "source_table": "link",
            "snowflake_table": "ods.link",
            "columns": [
                "link_id", "url", "title", "description", "views", "is_fetched",
                "created_by", "created_at", "link_embedding"
            ]
        },
        "user_info": {
            "source_table": "user_info",
            "snowflake_table": "ods.user_info",
            "columns": [
                "user_id", "username", "password", "user_embedding", "created_at"
            ]
        },
        "tag": {
            "source_table": "tag",
            "snowflake_table": "ods.tag",
            "columns": [
                "tag_id", "tag_name", "created_at"
            ]
        },
        "link_group": {
            "source_table": "link_group",
            "snowflake_table": "ods.link_group",
            "columns": [
                "group_id", "group_title", "created_at"
            ]
        },
        "link_user_map": {
            "source_table": "link_user_map",
            "snowflake_table": "ods.link_user_map",
            "columns": [
                "user_id", "link_id", "is_public", "created_at"
            ]
        },
        "link_history": {
            "source_table": "link_history",
            "snowflake_table": "ods.link_history",
            "columns": [
                "user_id", "link_id", "created_at"
            ]
        },
        "link_tag_map": {
            "source_table": "link_tag_map",
            "snowflake_table": "ods.link_tag_map",
            "columns": [
                "tag_id", "link_id"
            ]
        },
        "link_group_link_map": {
            "source_table": "link_group_link_map",
            "snowflake_table": "ods.link_group_link_map",
            "columns": [
                "link_id", "group_id", "created_at"
            ]
        }
    }
}

TARGET_TABLE = {
    "table_key" : "raw_data.link_detail",
    "columns": [
        "link_id", "subdomain", "host", "path", "parameters", "fragment"
    ]
}

with DAG(
    dag_id="oltp_to_olap_dag",
    start_date=None,
    catchup=False,
    schedule="@hourly",
    render_template_as_native_obj=True,
) as dag:
    tasks = {}
    for table_key, table_config in OLTP_CONFIG["table_config"].items():
        task = PostgresToSnowflakeOperator(
            task_id=f"transfer_{table_key}",
            postgres_conn_id="postgres_default",
            snowflake_conn_id="snowflake_default",
            snowflake_db="linkchain",
            snowflake_schema="ods",
            table_key=table_key,
            table_config=table_config,
            chunk_size="{{ var.json.oltp_to_olap.chunk_size }}",
            pool="oltp_to_olap_pool"
        )
        tasks[table_key] = task

    link_config = OLTP_CONFIG["table_config"]["link"]

    link_detail_dest_table = TARGET_TABLE["table_key"]
    link_detail_dest_columns = TARGET_TABLE["columns"]

    parse_link_detail = ParseLinkDetailOperator(
        task_id='parse_link_detail',
        snowflake_db = "linkchain",
        snowflake_conn_id="snowflake_default",
        source_table=link_config["snowflake_table"],
        source_columns=["link_id", "url"],
        dest_table=link_detail_dest_table,
        dest_columns=link_detail_dest_columns,
        chunk_size="{{ var.json.oltp_to_olap.chunk_size }}"
    )

    # task list내 모든 task 수행 + link task 이후에는 parse_link_detail 진행
    if 'link' in tasks:
        tasks['link'] >> parse_link_detail
