import pandas as pd
import numpy as np

from enum import Enum
from decimal import Decimal, getcontext
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field, InitVar
from datetime import datetime, date
from queue import PriorityQueue
from typing import Iterator, TypeVar, Generic, List, Dict
from bafrapy.repository.data_repo import DataRepository


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

@dataclass
class DataSet:
    asset: str
    resolution: int
    start_date: date
    end_date: date
    stream: Iterator[pd.DataFrame] = field(default=None, init=False)
    data_chunk: pd.DataFrame = field(default=None, init=False)
    chunk_iterator: Iterator[pd.DataFrame] = field(default=None, init=False)
    conn: DataRepository = field(default_factory=DataRepository, init=False)
    current_data: dict = field(default=None, init=False)

    def _initialize(self):
        self.stream = self.conn.get_data_stream(self.asset, self.resolution, self.start_date, self.end_date)
        self.data_chunk = next(self.stream, None)
        if self.data_chunk is None:
            raise ValueError("No data for the specified range")
        self.chunk_iterator = self.data_chunk.itertuples()

    def __post_init__(self):
        self._initialize()

    def get_current_data(self) -> OHLCV:
        return self.current_data

    def get_current_field(self, field: str) -> any: # Deprecate
        return self.current_data[field]
    
    def process_row(self, row: pd.Series) -> OHLCV:
        timestamp = None
        if isinstance(row[0], pd.Timestamp):
            timestamp = row[0].to_pydatetime()
        elif isinstance(row[0], (int, float)):
            timestamp = datetime.fromtimestamp(row[0])
        else:
            timestamp = row[0]
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
        self.current_data = next(self.chunk_iterator, None)
        if self.current_data is None:
            print("Siguiente chunk")
            self.data_chunk = next(self.stream, None)
            if self.data_chunk is not None:
                self.chunk_iterator = self.data_chunk.itertuples()
                return self.next_data()
            else:
                return None
        return self.process_row(self.current_data)
    

    def is_active(self) -> bool:
        return self.stream is not None and (self.data_chunk is not None or self.current_data is not None)

