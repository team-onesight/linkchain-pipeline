import random
import string
from typing import Any

import polars as pl
from hooks.abc.snowflake_base_hook import CustomSnowflakeBaseHook
from snowflake.connector.pandas_tools import write_pandas


class SnowflakeAggregationHook(CustomSnowflakeBaseHook):
    def __init__(
        self,
        snowflake_conn_id: str = "snowflake_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, snowflake_conn_id=snowflake_conn_id, **kwargs)
        self.database = "LINKCHAIN"
        self.schema = "ANALYTICS"

    def get_active_rules(self) -> pl.DataFrame:
        sql = """
            SELECT
            group_code
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
                , lc.url              AS url
                , lc.title            AS title
                , lc.description      AS description
                , ARRAY_AGG(t.tag_name) AS tag_list
            FROM analytics.link_clustered AS lc
                LEFT JOIN analytics.tag t ON lc.link_id = t.link_id
            GROUP BY lc.link_id, lc.url, lc.title, lc.description
        """
        return self._row_to_df(*self._execute_query(sql))

    def merge_link_groups(
        self, df: pl.DataFrame, target_table: str = "ANALYTICS.LINK_GROUP"
    ):
        """
        Polars DataFrame을 받아 link_group 테이블에 중복 없이 적재합니다.
        """
        if df.is_empty():
            self.log.info("Dataframe is empty, skipping merge.")
            return

        target_cols = ["link_id", "group_title"]
        pdf = df.select(target_cols).to_pandas()

        pdf.columns = [c.upper() for c in pdf.columns]

        conn = self.get_conn()
        cursor = conn.cursor()

        random_suffix = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=8)  # noqa: S311
        )
        temp_table = f"{target_table}_TEMP_{random_suffix}"

        try:
            self.log.info(f"Creating temp table: {temp_table}")
            cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} LIKE {target_table}")

            success, nchunks, nrows, _ = write_pandas(
                conn,
                pdf,
                table_name=temp_table.split(".")[-1],
                schema=temp_table.split(".")[-2] if "." in temp_table else None,
                quote_identifiers=False,
            )
            self.log.info(f"Staged {nrows} rows to {temp_table}")

            merge_query = f"""
            MERGE INTO {target_table} AS target
            USING {temp_table} AS source
            ON target.LINK_ID = source.LINK_ID
               AND target.GROUP_TITLE = source.GROUP_TITLE
            WHEN NOT MATCHED THEN
                INSERT (LINK_ID, GROUP_TITLE)
                VALUES (source.LINK_ID, source.GROUP_TITLE)
            """  # noqa: S608

            cursor.execute(merge_query)
            self.log.info(f"Merged data into {target_table} successfully.")

        finally:
            cursor.close()
            conn.close()

    def _execute_query(self, sql: str) -> tuple[Any, list[Any]]:
        with self.get_conn().cursor() as cur:
            self.log.debug(f"Executing query: {sql}")
            cur.execute(sql)
            results = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
        return results, col_names

    @staticmethod
    def _row_to_df(rows, col_names):
        if not rows:
            return pl.DataFrame([], schema=[c.lower() for c in col_names])
        df = pl.DataFrame(rows, schema=col_names, orient="row")
        df = df.rename({col: col.lower() for col in df.columns})
        return df
