from aggregator.processor.processors import (
    BaseStrategy,
    KeywordStrategy,
    TagMatchStrategy,
    VectorStrategy,
)


class ProcessorFactory:
    _strategies = {
        "KEYWORD_MATCH": KeywordStrategy(),
        "VECTOR_SIM": VectorStrategy(),  # Not implemented yet
        "TAG_MATCH": TagMatchStrategy(),
    }

    @classmethod
    def get_strategy(cls, rule_type: str) -> BaseStrategy:
        strategy = cls._strategies.get(rule_type, None)
        if not strategy:
            raise ValueError(f"Unsupported rule type: {rule_type}")
        return strategy
