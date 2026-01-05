from aggregator.processor.processors import (
    BaseStrategy,
    CompositeStrategy,
    KeywordStrategy,
    TagMatchStrategy,
    UrlMatchStrategy,
)


class ProcessorFactory:
    """
    다양한 처리 전략(Strategy)을 생성하는 팩토리 클래스
    1. KEYWORD_MATCH: 키워드 기반 필터링 전략
    2. TAG_MATCH: 태그 기반 필터링 전략
    3. URL_MATCH: URL 기반 필터링 전략
    4. COMPOSITE: 여러 전략을 조합한 복합 전략
    """

    _strategies = {
        "KEYWORD_MATCH": KeywordStrategy(),
        "TAG_MATCH": TagMatchStrategy(),
        "URL_MATCH": UrlMatchStrategy(),
        "COMPOSITE": CompositeStrategy(),
    }

    @classmethod
    def get_strategy(cls, rule_type: str) -> BaseStrategy:
        strategy = cls._strategies.get(rule_type, None)
        if not strategy:
            raise ValueError(f"Unsupported rule type: {rule_type}")
        return strategy
