from bafrapy.exchanges.client import ExchangeProvider, ExchangeSpotClient
from bafrapy.exchanges.clients.binance import BinanceClient


class ExchangeSpotClientFactory:
    __registry: dict[ExchangeProvider, type[ExchangeSpotClient]] = {ExchangeProvider.BINANCE: BinanceClient}

    def create_exchange_client(self, exchange: ExchangeProvider) -> ExchangeSpotClient:
        client_class = self.__registry.get(exchange)
        if client_class is None:
            raise ValueError(f"Exchange {exchange} not supported")
        return client_class()
