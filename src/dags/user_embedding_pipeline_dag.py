from datetime import datetime, timedelta

from airflow.sdk import DAG
from operators.calculate_user_embeddings_operator import (
    CalculateUserEmbeddingsOperator,
)
from operators.fetch_link_embeddings_operator import (
    FetchLinkEmbeddingsOperator,
)
from operators.upsert_user_embeddings_operator import (
    UpsertUserEmbeddingsOperator,
)

with DAG(
    dag_id="user_embedding_pipeline",
    description="user embedding pipeline (fetch → calculate → upsert) by Incremental",
    start_date=datetime(2026, 1, 4, 0, 30),
    schedule=timedelta(hours=1),
    catchup=False,
    tags=["embedding", "user", "incremental"],
) as dag:

    fetch_link_embeddings = FetchLinkEmbeddingsOperator(
        task_id="fetch_link_embeddings",
        source_schema="public",
        target_schema="staging",
        target_table="link_embedding_run",
    )

    calculate_user_embeddings = CalculateUserEmbeddingsOperator(
        task_id="calculate_user_embeddings",
        source_schema="staging",
        source_table="link_embedding_run",
        target_schema="analytics",
        target_table="user_embedding_state",
    )

    upsert_user_embeddings = UpsertUserEmbeddingsOperator(
        task_id="upsert_user_embeddings",
        source_schema="analytics",
        source_table="user_embedding_state",
        target_schema="public",
        target_table="user_info",
    )

    fetch_link_embeddings >> calculate_user_embeddings >> upsert_user_embeddings
