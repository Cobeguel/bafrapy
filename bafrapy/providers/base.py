from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from typing import List

import pandas as pd

from attrs import define, field
from requests import Request, Response, Session
from requests.exceptions import HTTPError, Timeout
from tenacity import (
    RetryError,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from bafrapy.logger import LoguruLogger as log


@define
class Symbol():
    symbol: str
    base: str
    quote: str
    min_lot: Decimal = field(default=Decimal(0))
    max_lot: Decimal = field(default=Decimal(0))
    lot_increment: Decimal = field(default=Decimal(0))


@define
class Resolution():
    name: str
    seconds: int


class Provider(ABC):
    _provider_name: str = field(alias="provider_name")

    @property
    def provider_name(self) -> str:
        return self._provider_name

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


class ProviderFactory(ABC):
    @abstractmethod
    def create_provider(self) -> Provider:
        ...


class ProviderConfig(ABC):
    @abstractmethod
    def load_config(cls, key: str, content: str):
        ...

@define
class BackoffConfig():
    timeout: int = field(default=10)
    backoff_factor: int = field(default=30)
    max_tries: int = field(default=3)
    giveup_codes: List[int] = field(default=[])


@define
class HTTPClient():
    _backoff_config: BackoffConfig = field(alias="backoff_config")
    _session: Session = field(factory=Session, init=False, repr=False)

    def _is_retryable(self, e: Exception) -> bool:
        return (
            isinstance(e, Timeout) or
            (isinstance(e, HTTPError) and e.response.status_code not in self._backoff_config.giveup_codes)
        )

    def _make_request(self, req: Request, raisable: bool = True) -> Response:
        prepared = self._session.prepare_request(req)
        log().debug(f"Requesting {prepared.url}")
        response = self._session.send(prepared, timeout=self._backoff_config.timeout)
        if raisable:
            response.raise_for_status()
        log().debug(f"Response {response.status_code} from {prepared.url}")
        return response

    def request(self, endpoint: str, method: str, params: dict = None, headers: dict = None, json: dict = None, retrayable: bool = True, raisable: bool = True) -> Response:
        req = Request(method, endpoint, params=params, headers=headers, json=json)

        if retrayable:
            retry_request = retry(
                stop=stop_after_attempt(self._backoff_config.max_tries),
                wait=wait_exponential(multiplier=self._backoff_config.backoff_factor),
                retry=retry_if_exception(self._is_retryable),
                reraise=True
            )
            try:
                return retry_request(self._make_request)(req, raisable=raisable)
            except RetryError as e:
                raise e.last_attempt.exception()

        else:
            return self._make_request(req, raisable=raisable)

