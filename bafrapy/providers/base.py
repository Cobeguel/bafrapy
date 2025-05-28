from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from typing import List

import pandas as pd
import requests

from attrs import define, field
from loguru import logger
from requests import Request, Response
from requests.exceptions import HTTPError
from tenacity import (
    RetryError,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)


@define()
class Symbol():
    _symbol: str = field(alias="symbol")
    _base: str = field(alias="base")
    _quote: str = field(alias="quote")
    _min_lot: Decimal = field(alias="min_lot", default=Decimal(0))
    _max_lot: Decimal = field(alias="max_lot", default=Decimal(0))
    _lot_increment: Decimal = field(alias="lot_increment", default=Decimal(0))

    @property
    def symbol(self) -> str:
        return self._symbol
    
    @symbol.setter
    def symbol(self, value: str):
        self._symbol = value

    @property
    def base(self) -> str:
        return self._base
    
    @base.setter
    def base(self, value: str):
        self._base = value
    
    @property
    def quote(self) -> str:
        return self._quote
    
    @quote.setter
    def quote(self, value: str):
        self._quote = value

    @property
    def min_lot(self) -> Decimal:
        return self._min_lot
    
    @min_lot.setter
    def min_lot(self, value: Decimal):
        self._min_lot = value
    
    @property
    def max_lot(self) -> Decimal:
        return self._max_lot

    @max_lot.setter
    def max_lot(self, value: Decimal):
        self._max_lot = value
    
    @property
    def lot_increment(self) -> Decimal:
        return self._lot_increment
    
    @lot_increment.setter
    def lot_increment(self, value: Decimal):
        self._lot_increment = value

@define
class SymbolDetail():
    _symbol: str = field(alias="symbol")
    _base: str = field(alias="base")


@define
class TabularOHLCV():
#External class
    OHLCV: pd.DataFrame = field(default=pd.DataFrame())


@define
class Resolution():
    _name: str = field(alias="name")
    _seconds: int = field(alias="seconds")

    @property
    def name(self) -> str:
        return self._name
    
    @property
    def seconds(self) -> int:
        return self._seconds
    

class Provider(ABC):

    @abstractmethod
    def list_available_symbols(self) -> List[Symbol]:
        ...
    @abstractmethod
    def symbol_first_date(self, symbol: str) -> datetime:
        ...
    @abstractmethod
    def symbol_last_date(self, symbol: str) -> datetime:
        ...
    @abstractmethod
    def get_day_data(self, symbol: str, day: date, resolution: Resolution) -> pd.DataFrame:
        ...
    @abstractmethod
    def get_month_data(self, symbol: str, month: date, resolution: Resolution) -> pd.DataFrame:
        ...


@define
class BackoffRequest(ABC):
    _timeout: int = field(alias="timeout")
    _max_tries: int = field(alias="max_tries")
    _giveup_codes: List[int] = field(alias="giveup_codes")

    def _is_retryable(self, e: Exception) -> bool:
        return isinstance(e, HTTPError) and e.response.status_code not in self.giveup_codes

    def _make_request(self, req: Request) -> Response:
        with requests.Session() as session:
            prepared = session.prepare_request(req)
            logger.debug(f"Requesting {prepared.url}")
            response = session.send(prepared, timeout=self.timeout)
            response.raise_for_status()
            return response

    def request(self, req: Request) -> Response:
        retry_request = retry(
            stop=stop_after_attempt(self.max_tries),
            wait=wait_exponential(multiplier=1),
            retry=retry_if_exception(self._is_retryable),
            reraise=True
        )

        try:
            return retry_request(self._make_request)(req)
        except RetryError as e:
            raise e.last_attempt.exception()


@define
class ProviderFactory(ABC):
    @abstractmethod
    def create_provider(self) -> Provider:
        ...
