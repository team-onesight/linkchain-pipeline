class ProcessorFactory:
    """
    다양한 처리 전략(Strategy)을 생성하는 팩토리 클래스
    """

    @classmethod
    def get_strategy(cls, rule_type: str):
        from aggregator.processor.processors import (
            CompositeStrategy,
            KeywordStrategy,
            TagMatchStrategy,
            UrlMatchStrategy,
        )

        _strategies = {
            "KEYWORD_MATCH": KeywordStrategy,
            "TAG_MATCH": TagMatchStrategy,
            "URL_MATCH": UrlMatchStrategy,
            "COMPOSITE": CompositeStrategy,
        }

        strategy_cls = _strategies.get(rule_type)
        if not strategy_cls:
            raise ValueError(f"Unsupported rule type: {rule_type}")

        return strategy_cls()
