import pandas as pd
import backoff
import requests

from dataclasses import dataclass
from typing import Dict, List
from datetime import date
from dataclasses import dataclass, field
from typing import Generator, Tuple
from requests.exceptions import HTTPError
from abc import ABC, abstractmethod
from frozendict import frozendict
from enum import Enum

@dataclass(frozen=True)
class Resolution:
    seconds: int
    name: str

resolutions = frozendict({
    1       : Resolution(seconds=1, name='1s'),
    60      : Resolution(seconds=60, name='1m'),
    300     : Resolution(seconds=300, name='5m'),
    900     : Resolution(seconds=900, name='15m'),
    1800    : Resolution(seconds=1800, name='30m'),
    3600    : Resolution(seconds=3600, name='1h'),
    14400   : Resolution(seconds=14400, name='4h'),
    86400   : Resolution(seconds=86400, name='1d')
})

find_by_name = lambda name: next((res for res in resolutions.values() if res.name == name), None)
find_by_seconds = lambda seconds: next((res for res in resolutions.values() if res.seconds == seconds), None)

@dataclass
class ResolutionManager():
    resolutions: frozendict[Dict[int, Resolution]] = field(default=resolutions)

    @property
    def names(self) -> List[str]:
        return [res.name for res in self.resolutions.values]

    def get_resolution_by_name(self, name: str) -> Resolution:
        return next((res for res in resolutions.values() if res.name == name), None)

    def get_next_resolution(self, current: Resolution) -> Resolution:
        sorted_resolutions = sorted(self.resolutions.values(), key=lambda res: res.seconds)
        current_index = sorted_resolutions.index(current)
        if current_index + 1 >= len(sorted_resolutions):
            return None
        return sorted_resolutions[current_index + 1]


_give_up = lambda e: e.response is not None and 500 <= e.response.status_code < 600

@dataclass
class BaseProvider(ABC):

    @abstractmethod
    def _handle_response(self, response: requests.Response) -> pd.DataFrame:
        pass

    @backoff.on_exception(backoff.expo, HTTPError, max_tries=4, giveup=_give_up)
    def _request_data(self, req: requests.Request) -> pd.DataFrame:
        with requests.Session() as session:
            prepared_req = session.prepare_request(req)
            response = session.send(prepared_req, timeout=300)
            response.raise_for_status()
            return self._handle_response(response)

    @abstractmethod
    def list_available_symbols(self) -> List[str]:
        pass

    @abstractmethod
    def get_data_resolutions(self, symbol: str) -> List[Resolution]:
        pass

    @abstractmethod
    def get_first_available_date(self, symbol: str) -> date:
        pass

    @abstractmethod
    def availability(self, symbol) -> Tuple[date, date]:
        pass

    @abstractmethod
    def get_data(self, symbol: str, start_date: date, end_date: date) -> Generator[pd.DataFrame, None, None]:
        pass

class Providers(Enum):
    BINANCE = 0

class ProviderFactory():
    @staticmethod
    def get_provider(provider: Providers) -> BaseProvider:
        if provider == Providers.BINANCE:
            from bafrapy.data_provider.binance_provider import BinanceProvider
            return BinanceProvider()
        else:
            raise ValueError(f'Provider {provider} not found')
