import logging
from abc import ABC, abstractmethod

import polars as pl
from airflow.exceptions import AirflowSkipException


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

    def _ensure_not_empty(
        self, df: pl.DataFrame, context_msg: str = ""
    ) -> pl.DataFrame:
        """결과가 비어있으면 AirflowSkipException을 던지는 공통 메서드"""
        if df.is_empty():
            self.log.info(f"Skipping: {context_msg} produced no results.")
            raise AirflowSkipException(f"No results found for {context_msg}")
        return df
