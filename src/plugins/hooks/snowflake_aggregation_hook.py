from typing import Any

import polars as pl
from hooks.abc.snowflake_base_hook import CustomSnowflakeBaseHook


class SnowflakeAggregationHook(CustomSnowflakeBaseHook):
    def __init__(
        self,
        *args,
        snowflake_conn_id: str = "snowflake_default",
        **kwargs,
    ):
        super().__init__(*args, snowflake_conn_id=snowflake_conn_id, **kwargs)
        self.database = "LINKCHAIN"
        self.schema = "ANALYTICS"

    def get_active_rules(self) -> pl.DataFrame:
        sql = """
        SELECT group_code
        , group_title
        , rule_type
        , rule_params
        FROM analytics.group_rules
        WHERE is_active = TRUE
    """

        return self._row_to_df(*self._execute_query(sql))

    def get_link_clustered(self) -> pl.DataFrame:
        sql = """
        SELECT
            lc.link_id            AS link_id
            , lc.url                AS url
            , lc.title              AS title
            , lc.description        AS description
            , ARRAY_AGG(t.tag_name) AS tag_list
        FROM
            analytics.link_clustered AS lc
                LEFT JOIN analytics.tag t ON lc.link_id = t.link_id
        GROUP BY
            lc.link_id, lc.url, lc.title, lc.description

    """

        return self._row_to_df(*self._execute_query(sql))

    def _execute_query(self, sql: str) -> tuple[Any, list[Any]]:
        with self.get_conn().cursor() as cur:
            self.log.debug(f"Executing query: {sql}")
            cur.execute(sql)
            results = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
        return results, col_names

    @staticmethod
    def _row_to_df(rows, col_names):
        df = pl.DataFrame(rows, schema=col_names, orient="row")
        df = df.rename({col: col.lower() for col in df.columns})

        return df
