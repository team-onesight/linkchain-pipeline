import polars as pl
from aggregator.processor.abc.base_strategy import BaseStrategy
from aggregator.processor.params import (
    CompositeParams,
    KeywordParams,
    TagMatchParams,
    UrlMatchParams,
)
from airflow.exceptions import AirflowSkipException


class KeywordStrategy(BaseStrategy):
    """
    키워드 기반 필터링 전략
    1. keywords: List[str] - 검색할 키워드 리스트
    2. operator: Literal["AND", "OR"] - 키워드 결합 방식
    3. title, description 컬럼에서 키워드 검색
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def process(self, df: pl.DataFrame, params: dict) -> pl.DataFrame:
        config = KeywordParams(**params)

        search_cols = [pl.col("title"), pl.col("description")]

        keyword_filters = []
        for kw in config.keywords:
            term_hit = pl.any_horizontal(
                [c.str.contains(f"(?i){kw}") for c in search_cols]
            )
            keyword_filters.append(term_hit)

        if config.operator == "AND":
            final_filter = pl.all_horizontal(keyword_filters)
        else:
            final_filter = pl.any_horizontal(keyword_filters)

        return df.filter(final_filter)


class TagMatchStrategy(BaseStrategy):
    """
    태그 기반 필터링 전략
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def process(self, df: pl.DataFrame, params: dict) -> pl.DataFrame:
        config = TagMatchParams(**params)

        if "tag_list" not in df.columns:
            self.log.warning(
                "[TagMatchStrategy] tag_list column not found in DataFrame."
            )
            return df.clear()

        if df.schema["tag_list"] in (pl.String, pl.Utf8):
            try:
                df = df.with_columns(
                    pl.col("tag_list").str.json_decode(dtype=pl.List(pl.String))
                )
            except Exception as e:
                self.log.error(
                    f"[TagMatchStrategy] JSON parsing failed for tag_list: {e}"
                )
                return df.clear()

        tag_filters = []

        lower_tag_col = pl.col("tag_list").list.eval(pl.element().str.to_lowercase())

        for tag in config.tags:
            target_tag = tag.lower()
            is_exist = lower_tag_col.list.contains(target_tag)
            tag_filters.append(is_exist)

        if config.operator == "AND":
            final_filter = pl.all_horizontal(tag_filters)
        else:
            final_filter = pl.any_horizontal(tag_filters)

        return df.filter(final_filter)


class UrlMatchStrategy(BaseStrategy):
    """
    URL 기반 필터링 전략
    1. url_patterns: List[str] - URL에 포함될 문자열(도메인, 경로 등)
    2. operator: Literal["AND", "OR"] - 패턴 결합 방식
    3. url 컬럼에서 패턴 검색 (대소문자 무시)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def process(self, df: pl.DataFrame, params: dict) -> pl.DataFrame:
        config = UrlMatchParams(**params)

        if "url" not in df.columns:
            self.log.warning("[UrlMatchStrategy] 'url' column not found in DataFrame.")
            return df.clear()

        url_filters = []

        for pattern in config.url_patterns:
            match_expr = pl.col("url").str.contains(f"(?i){pattern}")
            url_filters.append(match_expr)

        if config.operator == "AND":
            final_filter = pl.all_horizontal(url_filters)
        else:
            final_filter = pl.any_horizontal(url_filters)

        return df.filter(final_filter)


class CompositeStrategy(BaseStrategy):
    """
    여러 전략을 조합하여 실행하는 복합 전략
    예: (Keyword 필터링) AND (URL 필터링)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def process(self, df: pl.DataFrame, params: dict) -> pl.DataFrame:
        config = CompositeParams(**params)
        strategy_map = {
            "KEYWORD_MATCH": KeywordStrategy,
            "TAG_MATCH": TagMatchStrategy,
            "URL_MATCH": UrlMatchStrategy,
        }

        if df.is_empty():
            return df

        if config.operator == "AND":
            current_df = df
            for rule in config.rules:
                strategy_cls = strategy_map.get(rule.rule_type)
                if not strategy_cls:
                    self.log.error(
                        f"Unknown rule type in CompositeStrategy: {rule.rule_type}"
                    )
                    raise AirflowSkipException(f"Unknown rule type: {rule.rule_type}")

                strategy_instance = strategy_cls()
                current_df = strategy_instance.process(current_df, rule.rule_params)

                if current_df.is_empty():
                    return current_df

            return current_df

        else:
            result_dfs = []
            for rule in config.rules:
                strategy_cls = strategy_map.get(rule.rule_type)
                if not strategy_cls:
                    continue

                strategy_instance = strategy_cls()
                filtered = strategy_instance.process(df, rule.rule_params)
                if not filtered.is_empty():
                    result_dfs.append(filtered)

            if not result_dfs:
                return df.clear()

            merged_df = pl.concat(result_dfs)
            return merged_df.unique(subset=["link_id"])
