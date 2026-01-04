from __future__ import annotations
import json
from typing import List, Tuple

import numpy as np
from airflow.sdk.bases.operator import BaseOperator
from hooks.postgres_transactional_hook import PostgresTransactionalHook

VECTOR_DIM = 768


class UpsertUserEmbeddingsOperator(BaseOperator):
    """
    analytics.user_embedding_state → public.user_info.user_embedding 반영
    """

    template_fields = ()

    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        source_schema: str = "analytics",
        source_table: str = "user_embedding_state",
        target_schema: str = "public",
        target_table: str = "user_info",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_schema = source_schema
        self.source_table = source_table
        self.target_schema = target_schema
        self.target_table = target_table

    def execute(self, context):
        hook = PostgresTransactionalHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info(f"Start upserting user embeddings into {self.target_schema}.{self.target_table}")

        # 1. analytics.user_embedding_state 조회
        sql_select = f"""
            SELECT user_id, user_embedding
            FROM {self.source_schema}.{self.source_table}
        """
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_select)
                rows = cur.fetchall()

        if not rows:
            self.log.info("No user_embedding_state rows found")
            return

        # 2. UPDATE용 파라미터 준비
        update_params: List[Tuple] = []
        for user_id, embedding in rows:
            try:
                if isinstance(embedding, str):
                    embedding_list = json.loads(embedding)
                else:
                    embedding_list = embedding

                vector = np.array(embedding_list, dtype=np.float32)

                if vector.shape[0] != VECTOR_DIM:
                    self.log.warning("Skipping invalid embedding dim for user_id=%s", user_id)
                    continue

                norm = np.linalg.norm(vector)
                if norm == 0.0:
                    continue

                normalized = (vector / norm).tolist()
                update_params.append((normalized, user_id))
            except Exception as e:
                self.log.warning("Skipping invalid embedding for user_id=%s: %s", user_id, e)
                continue

        if not update_params:
            self.log.info("No valid embeddings to update")
            return

        # 3. user_info UPDATE
        sql_update = f"""
            UPDATE {self.target_schema}.{self.target_table}
            SET user_embedding = %s
            WHERE user_id = %s
        """
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql_update, update_params)
                conn.commit()

        self.log.info("Updated user_embedding for %d users", len(update_params))
