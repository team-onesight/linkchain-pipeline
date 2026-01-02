import logging
from abc import ABC, abstractmethod

import polars as pl


class BaseStrategy(ABC):
    log = None

    def __init__(self, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def process(self, df: pl.DataFrame, params: dict) -> pl.DataFrame:
        """
        to return filtered DataFrame based on strategy and params
        1. df: input DataFrame to be processed
        2. params: dict of parameters specific to the strategy
        3. return: filtered DataFrame
        """
        raise NotImplementedError("Subclasses must implement this method")
