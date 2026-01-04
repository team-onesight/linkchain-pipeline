from __future__ import annotations
import json
from collections import defaultdict
from typing import Dict, Tuple, List

import numpy as np
from airflow.sdk.bases.operator import BaseOperator
from hooks.postgres_transactional_hook import PostgresTransactionalHook


class CalculateUserEmbeddingsOperator(BaseOperator):
    """
    staging.link_embedding_run의 증분 데이터를 이용하여
    user_embedding running mean을 계산하고
    analytics.user_embedding_state에 upsert하는 Operator
    """

    template_fields = ()

    def __init__(
        self,
        source_schema: str = "staging",
        source_table: str = "link_embedding_run",
        target_schema: str = "analytics",
        target_table: str = "user_embedding_state",
        postgres_conn_id: str = "postgres_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_schema = source_schema
        self.source_table = source_table
        self.target_schema = target_schema
        self.target_table = target_table
        self.postgres_conn_id = postgres_conn_id

    @staticmethod
    def _l2_normalize(vec: np.ndarray) -> np.ndarray:
        norm = np.linalg.norm(vec)
        if norm == 0.0:
            return vec
        return vec / norm

    def execute(self, context):
        self.log.info("Start calculating user embeddings (running mean)")

        hook = PostgresTransactionalHook(postgres_conn_id=self.postgres_conn_id)

        # incremental link_embedding 조회
        incremental: Dict[int, List[np.ndarray]] = defaultdict(list)
        sql_select = f"""
            SELECT user_id, link_embedding
            FROM {self.source_schema}.{self.source_table}
        """
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_select)
                rows = cur.fetchall()

        if not rows:
            self.log.info("No incremental link embeddings found")
            return

        # 평균 계산을 위한 type 변환
        for user_id, embedding in rows:
            try:
                if isinstance(embedding, str):
                    embedding_list = json.loads(embedding)
                else:
                    embedding_list = embedding

                vec = np.array(embedding_list, dtype=np.float32)
                incremental[user_id].append(vec)
            except Exception as e:
                self.log.warning(
                    "Skipping invalid embedding for user_id=%s: %s (%s)",
                    user_id,
                    embedding,
                    e,
                )
                continue

        if not incremental:
            self.log.info("No valid embeddings to process")
            return

        user_ids = tuple(incremental.keys())

        # 기존 user_embedding 조회
        existing_map: Dict[int, Tuple[np.ndarray, int]] = {}
        sql_existing = f"""
            SELECT user_id, user_embedding, cnt
            FROM {self.target_schema}.{self.target_table}
            WHERE user_id IN %s
        """
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_existing, (user_ids,))
                state_rows = cur.fetchall()

        for user_id, embedding, cnt in state_rows:
            try:
                if isinstance(embedding, str):
                    embedding_list = json.loads(embedding)
                else:
                    embedding_list = embedding
                existing_map[user_id] = (np.array(embedding_list, dtype=np.float32), cnt)
            except Exception as e:
                self.log.warning(
                    "Skipping invalid existing embedding for user_id=%s: %s (%s)",
                    user_id,
                    embedding,
                    e,
                )

        # 기존 평균과 incremental에 대한 평균 계산 및 정규화
        upsert_params: List[Tuple] = []
        for user_id, vectors in incremental.items():
            new_cnt = len(vectors)
            new_sum = np.sum(vectors, axis=0)

            if user_id in existing_map:
                prev_embedding, prev_cnt = existing_map[user_id]
                total_sum = prev_embedding * prev_cnt + new_sum
                total_cnt = prev_cnt + new_cnt
            else:
                total_sum = new_sum
                total_cnt = new_cnt

            mean_vec = total_sum / total_cnt
            normalized_vec = self._l2_normalize(mean_vec)

            upsert_params.append((user_id, normalized_vec.tolist(), total_cnt))

        # upsert
        upsert_sql = f"""
            INSERT INTO {self.target_schema}.{self.target_table} (
                user_id,
                user_embedding,
                cnt,
                updated_at
            )
            VALUES (%s, %s, %s, now())
            ON CONFLICT (user_id)
            DO UPDATE SET
                user_embedding = EXCLUDED.user_embedding,
                cnt = EXCLUDED.cnt,
                updated_at = now()
        """
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(upsert_sql, upsert_params)
                conn.commit()

        self.log.info("Finished calculating user embeddings")
