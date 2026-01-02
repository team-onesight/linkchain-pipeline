import polars as pl
from aggregator.processor.abc.base_strategy import BaseStrategy
from aggregator.processor.params import KeywordParams, TagMatchParams, VectorParams


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


class VectorStrategy(BaseStrategy):
    """
    벡터 유사도 기반 필터링 전략
    1. threshold: float - 유사도 임계값 (0.0 ~ 1.0)
    2. ref_content_id: str - 참조 콘텐츠 ID
    3. title, description 컬럼에서 벡터 유사도 계산 후 필터링
    4. (현재는 더미 구현으로 상위 5개 행 반환)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def process(self, df: pl.DataFrame, params: dict) -> pl.DataFrame:
        config = VectorParams(**params)
        print(f"DEBUG: Vector Search 실행 (Threshold: {config.threshold})")
        # TODO: 실제 벡터 유사도 계산 로직 구현
        return df.head(5)
