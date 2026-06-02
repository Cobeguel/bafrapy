import ccxt

from attrs import define, field

from bafrapy.exchanges.clients.parser import MARKET_PARSER, DecimalConverter
from bafrapy.exchanges.markets import MarketResponse


@define(slots=False, kw_only=False)
class BinanceClient:
    _exchange: ccxt.Exchange = field(factory=lambda: ccxt.binance({"enableRateLimit": True}))

    def get_markets(self) -> list[MarketResponse]:
        converter = DecimalConverter(prefer_attrib_converters=True)
        markets: list[MarketResponse] = []

        for market in self._exchange.load_markets().values():
            if market.get("type") != "spot" or not market.get("base") or not market.get("quote"):
                continue

            parsed = MARKET_PARSER.search(market)
            if not parsed or not parsed.get("base") or not parsed.get("quote"):
                continue

            markets.append(converter.structure(parsed, MarketResponse))

        return markets
