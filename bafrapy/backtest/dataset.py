from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, getcontext
from typing import Generic, Iterator, TypeVar

import pandas as pd

from loguru import logger

from bafrapy.models.data.repo import DataRepository

T = TypeVar('T')

def set_context(precision: int):
    getcontext().prec = precision

@dataclass
class PeekIterator(Generic[T]):
    iterator: Iterator[T]
    next_element: T = field(default=None)

    def __post_init__(self):
        self.next_element = next(self.iterator, None)

    def peek(self) -> T:
        return self.next_element

    def __iter__(self) -> 'PeekIterator':
        return self

    def __next__(self) -> T:
        if self.next_element is None:
            raise StopIteration
        current_element = self.next_element
        self.next_element = next(self.iterator, None)
        return current_element

@dataclass
class OHLCV:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    custom: dict = field(default_factory=dict)

    def __post_init__(self):
        self.open = Decimal(self.open)
        self.high = Decimal(self.high)
        self.low = Decimal(self.low)
        self.close = Decimal(self.close)
        self.volume = Decimal(self.volume)     

@dataclass
class DataSet(metaclass=ABCMeta):
    resolution: int
    current_data: OHLCV = field(default=None, init=False)

    def get_current_data(self) -> OHLCV:
        return self.current_data

    @abstractmethod
    def process_row(self, row: Iterable[object]) -> OHLCV:
        pass
    
    @abstractmethod
    def next_data(self) -> OHLCV:
        pass

    @abstractmethod
    def has_data(self) -> bool:
        pass


@dataclass
class DataSetPandas(DataSet):
    data: pd.DataFrame = field(default=None)
    row_iterator: Iterator[pd.DataFrame] = field(default_factory=lambda: iter([pd.DataFrame()]), init=False)

    def __post_init__(self):
        self.data = self.data.reset_index(drop=True)
        self.row_iterator = iter(self.data.itertuples(index=False))

    def process_row(self, row: pd.Series) -> OHLCV:
        timestamp = row[0]
        if isinstance(row[0], pd.Timestamp):
            timestamp = row[0].to_pydatetime()
        elif isinstance(row[0], (int, float)):
            timestamp = datetime.fromtimestamp(row[0])
        return OHLCV(
            timestamp=timestamp,
            open=Decimal(row[1]),
            high=Decimal(row[2]),
            low=Decimal(row[3]),
            close=Decimal(row[4]),
            volume=Decimal(row[5]),
            custom={row._fields[i]: getattr(row, row._fields[i]) for i in range(6, len(row._fields))}
        )

    def next_data(self) -> OHLCV:
        try:
            self.current_data = self.process_row(next(self.row_iterator))
            return self.current_data
        except StopIteration:
            return None

    def has_data(self) -> bool:
        return self.data is not None and not self.data.empty


@dataclass
class DataSetClickhouse(DataSetPandas):
    start_date: date
    end_date: date
    asset: str
    stream: Iterator[pd.DataFrame] = field(default=None, init=False)
    data: pd.DataFrame = field(default=None, init=False)
    chunk_iterator: Iterator[pd.DataFrame] = field(default=None, init=False)
    conn: DataRepository = field(default_factory=DataRepository, init=False)

    def _initialize(self):
        self.stream = self.conn.get_data_stream(self.asset, self.resolution, self.start_date, self.end_date)
        self.data = next(self.stream, None)
        if self.data is None:
            raise ValueError(f"No data for {self.asset} by the specified range: {self.start_date} - {self.end_date}")
        self.chunk_iterator = self.data.itertuples()

        logger.info(f"DataSetClickhouse initialized: {self.asset} - {self.start_date} - {self.end_date}")

    def __post_init__(self):
        self._initialize()

    def next_data(self) -> OHLCV:
        self.current_data = next(self.chunk_iterator, None)
        if self.current_data is None:
            self.data = next(self.stream, None)
            if self.data is not None:
                self.chunk_iterator = self.data.itertuples()
                return self.next_data()
            else:
                return None
        return self.process_row(self.current_data)
    
    def has_data(self) -> bool:
        return self.stream is not None and (self.data is not None or self.current_data is not None)
