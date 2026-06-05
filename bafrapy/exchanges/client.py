from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import List

import polars as pl

from attrs import define

from bafrapy.exchanges.markets import MarketResponse


class ExchangeProvider(Enum):
    BINANCE = "binance"


@define(frozen=True)
class ExchangeClientResolution:
    seconds: int
    name: str


class ExchangeSpotClient(ABC):
    _exchange_name: str

    @property
    def provider_name(self) -> str:
        return self._exchange_name

    @abstractmethod
    def resolve_symbol(self, base: str, quote: str) -> str: ...
    @abstractmethod
    def get_markets(self) -> List[MarketResponse]: ...
    @abstractmethod
    def get_first_market_date(self, symbol: str) -> datetime: ...
    @abstractmethod
    def get_last_market_date(self, symbol: str) -> datetime: ...
    @abstractmethod
    def get_ohlcv(
        self,
        base: str,
        quote: str,
        resolution: ExchangeClientResolution,
        start: datetime,
        end: datetime,
        chunk_size=50000,
    ) -> pl.DataFrame: ...
