from abc import ABC, abstractmethod
from datetime import date
from typing import Iterator, List, Optional

import polars as pl

from attrs import define


@define(frozen=True)
class Market:
    exchange: str
    symbol: str
    base: str
    quote: str


@define(frozen=True)
class HistoricalRange:
    market: Market
    start: date
    end: date


class OHLCVRepository(ABC):
    @abstractmethod
    def list_exchanges(self) -> List[str]:
        pass

    @abstractmethod
    def list_symbols(self, exchange: str) -> List[str]:
        pass

    @abstractmethod
    def market_historical_range(self, exchange: str, symbol: str, resolution: int) -> Optional[HistoricalRange]:
        pass

    @abstractmethod
    def insert_ohlcv(self, ohlcv: pl.DataFrame):
        pass

    @abstractmethod
    def get_ohlcv(self, exchange: str, symbol: str, resolution: int, start: date, end: date) -> pl.DataFrame:
        pass

    @abstractmethod
    def get_ohlcv_stream(
        self, exchange: str, symbol: str, resolution: int, start: date, end: date, chunk_size: int = 100000
    ) -> Iterator[pl.DataFrame]:
        pass
