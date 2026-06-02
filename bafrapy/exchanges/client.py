from enum import Enum
from typing import List, Protocol

from bafrapy.exchanges.clients.binance import BinanceClient
from bafrapy.exchanges.markets import MarketResponse


class ExchangeProvider(Enum):
    BINANCE = "binance"


class ExchangeClient(Protocol):
    def get_markets(self) -> List[MarketResponse]: ...


class ExchangeClientFactory:
    __registry: dict[ExchangeProvider, type[ExchangeClient]] = {ExchangeProvider.BINANCE: BinanceClient}

    def create_exchange_client(self, exchange: ExchangeProvider) -> ExchangeClient:
        client_class = self.__registry.get(exchange)
        if client_class is None:
            raise ValueError(f"Exchange {exchange} not supported")
        return client_class()
