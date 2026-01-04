from datetime import timedelta
from airflow.sdk.bases.operator import BaseOperator
from airflow.utils.context import Context

from hooks.postgres_transactional_hook import PostgresTransactionalHook


class FetchLinkEmbeddingsOperator(BaseOperator):
    """
    link_user_map 기준으로 incremental하게
    user_id별 link_embedding을 fetch하여 staging 테이블에 적재하는 Operator
    """

    template_fields = ("_sql",)

    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        source_schema: str = "public",
        target_schema: str = "staging",
        target_table:str = "link_embedding_run",
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.source_schema = source_schema
        self.target_schema = target_schema

        self._sql = f"""
        TRUNCATE {target_schema}.{target_table} ;
        INSERT INTO {target_schema}.{target_table} (
            run_id,
            user_id,
            link_id,
            link_embedding
        )
        SELECT
            %(run_id)s            AS run_id,
            lum.user_id           AS user_id,
            l.link_id             AS link_id,
            l.link_embedding      AS link_embedding
        FROM {source_schema}.link_user_map lum
        JOIN {source_schema}.link l
          ON lum.link_id = l.link_id
        WHERE
            lum.created_at >= %(start_ts)s
            AND lum.created_at <  %(end_ts)s
            AND l.link_embedding IS NOT NULL;
        """

    def execute(self, context: Context):
        hook = PostgresTransactionalHook(
            postgres_conn_id=self.postgres_conn_id,
            schema=self.source_schema,
        )

        run_id = context["dag_run"].run_id
        start_ts = context["data_interval_start"]
        end_ts = context["data_interval_end"]

        if start_ts == end_ts:  # test용 manual trigger
            self.log.warning("Manual trigger detected. Using last 1 hour.")
            end_ts = context.get("logical_date") or start_ts
            start_ts = end_ts - timedelta(hours=1)

        self.log.info(
            "Fetching link embeddings incrementally: %s ~ %s",
            start_ts,
            end_ts,
        )

        hook.execute(
            sql=self._sql,
            params={
                "run_id": run_id,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        )

        self.log.info("Staging link embeddings completed for run_id=%s", run_id)
