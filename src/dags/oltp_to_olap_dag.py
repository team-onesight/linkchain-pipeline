from airflow.sdk import DAG, Variable
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

    for table_key, table_config in config["table_config"].items():
        PostgresToSnowflakeOperator(
            task_id=f"transfer_{table_key}",
            postgres_conn_id="postgres_default",
            snowflake_conn_id="snowflake_default",
            snowflake_db="linkchain",
            snowflake_schema="ods",
            table_key=table_key,
            table_config=table_config,
            chunk_size=config["chunk_size"],
            pool="oltp_to_olap_pool"
        )
