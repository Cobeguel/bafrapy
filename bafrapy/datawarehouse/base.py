from abc import ABC, abstractmethod
from datetime import date
from typing import Iterator, List

import pandas as pd

from attrs import define


@define(frozen=True)
class Symbol:
    provider: str
    symbol: str


@define(frozen=True)
class RowsResolution:
    resolution: int
    num_rows: int


@define(frozen=True)
class SymbolStats(Symbol):
    rows_resolutions: List[RowsResolution]


@define(frozen=True)
class DateCount(Symbol):
    date_count: date
    rows_resolutions: List[RowsResolution]


@define(frozen=True)
class SymbolAvailability:
    provider: str
    symbol: str
    resolution: int
    first_date: date
    last_date: date


class OHLCVRepository(ABC):
    _ORIGINAL_STATE = 'ORIGINAL'
    _GAP_STATE = 'GAP'
    
    @abstractmethod
    def provider_symbols_stats(self, provider: str) -> List[SymbolStats]:
        pass

    @abstractmethod
    def symbol_stats(self, provider: str, symbol: str) -> SymbolStats:
        pass

    @abstractmethod
    def list_providers(self) -> List[str]:
        pass

    @abstractmethod
    def list_symbols(self, provider: str) -> List[str]:
        pass

    @abstractmethod
    def symbol_availability(self, provider: str, symbol: str, resolution: int) -> SymbolAvailability:
        pass

    @abstractmethod
    def count_rows(self, provider: str = "", symbol: str = "", resolution: int = 0) -> int:
        pass

    @abstractmethod
    def insert_data(self, data: pd.DataFrame, fill_gaps: bool = False):
        pass

    @abstractmethod
    def get_by_range(self, provider: str, symbol: str, resolution: int, start: date, end: date) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_by_range_stream(self, provider: str, symbol: str, resolution: int, start: date, end: date) -> Iterator[pd.DataFrame]:
        pass

    @abstractmethod
    def clean_or_optimize_provider(self, provider: str):
        pass

    @abstractmethod
    def clean_or_optimize_symbol(self, provider: str, symbol: str):
        pass
