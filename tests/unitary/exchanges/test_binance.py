from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import Mock

import polars as pl
import pytest

from bafrapy.exchanges.client import ExchangeClientResolution
from bafrapy.exchanges.clients.binance import BinanceClient
from bafrapy.exchanges.clients.parser import MARKET_PARSER, DecimalConverter
from bafrapy.exchanges.markets import MarketResponse


def _raw_ohlcv_row(open_time: str = "1748131200000") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "time": [open_time],
            "open": ["1.1000"],
            "high": ["1.2000"],
            "low": ["1.0000"],
            "close": ["1.1500"],
            "volume": ["1800.000"],
            "quote_volume": ["2000.00"],
        }
    )


@pytest.fixture
def exchange() -> Mock:
    return Mock()


@pytest.fixture
def client(exchange: Mock) -> BinanceClient:
    return BinanceClient(data_url="https://data.bafrapy-test.com", exchange=exchange)


class TestBinanceClientHelpers:
    def test_exchange_name(self, client: BinanceClient) -> None:
        assert client.exchange_name == "binance"

    def test_build_symbol(self, client: BinanceClient) -> None:
        assert client.build_symbol("BTC", "USDT") == "BTCUSDT"

    def test_daily_url(self, client: BinanceClient) -> None:
        assert (
            client._daily_url("BTCUSDT", "1m", date(2024, 5, 25))
            == "https://data.bafrapy-test.com/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-05-25.zip"
        )

    def test_monthly_url(self, client: BinanceClient) -> None:
        assert (
            client._monthly_url("BTCUSDT", "1m", date(2024, 5, 1))
            == "https://data.bafrapy-test.com/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-05.zip"
        )


class TestBinanceClientAggregateTrades:
    def test_get_first_market_date(self, client: BinanceClient, exchange: Mock) -> None:
        exchange.publicGetAggTrades.return_value = [{"T": 1716819554000}]

        result = client.get_first_market_date("BTC", "USDT")

        assert result == datetime(2024, 5, 27, 14, 19, 14, tzinfo=timezone.utc)
        exchange.publicGetAggTrades.assert_called_once_with({"symbol": "BTCUSDT", "fromId": 0, "limit": 1})

    def test_get_last_market_date(self, client: BinanceClient, exchange: Mock) -> None:
        exchange.publicGetAggTrades.return_value = [{"T": 1716819554000}]

        result = client.get_last_market_date("BTC", "USDT")

        assert result == datetime(2024, 5, 27, 14, 19, 14, tzinfo=timezone.utc)
        exchange.publicGetAggTrades.assert_called_once_with({"symbol": "BTCUSDT", "limit": 1})

    def test_get_agg_trade_timestamp_supports_microseconds(self, client: BinanceClient, exchange: Mock) -> None:
        exchange.publicGetAggTrades.return_value = [{"T": 1735689600000000}]

        result = client.get_last_market_date("BTC", "USDT")

        assert result == datetime(2025, 1, 1, tzinfo=timezone.utc)

    def test_get_agg_trade_timestamp_raises_when_no_trades(self, client: BinanceClient, exchange: Mock) -> None:
        exchange.publicGetAggTrades.return_value = []

        with pytest.raises(ValueError, match="No aggregate trades found for symbol BTCUSDT"):
            client._get_agg_trade_timestamp("BTCUSDT", {"limit": 1})


class TestBinanceClientMarkets:
    def test_get_markets_filters_spot_markets_and_structures_limits(
        self, client: BinanceClient, exchange: Mock
    ) -> None:
        exchange.load_markets.return_value = {
            "BTC/USDT": {
                "symbol": "BTC/USDT",
                "type": "spot",
                "base": "BTC",
                "quote": "USDT",
                "limits": {
                    "amount": {"min": "0.00001", "max": "1000"},
                    "price": {"min": "0.01", "max": "1000000"},
                    "cost": {"min": "10", "max": "100000"},
                    "market": {"min": "5", "max": "50000"},
                },
            },
            "ETH/USDT:USDT": {
                "symbol": "ETH/USDT:USDT",
                "type": "swap",
                "base": "ETH",
                "quote": "USDT",
                "limits": {},
            },
            "BROKEN/USDT": {
                "symbol": "BROKEN/USDT",
                "type": "spot",
                "base": None,
                "quote": "USDT",
                "limits": {},
            },
        }

        markets = client.get_markets()

        assert len(markets) == 1
        market = markets[0]
        assert market.raw_symbol == "BTC/USDT"
        assert market.symbol == "BTCUSDT"
        assert market.amount_min == Decimal("0.00001")
        assert market.amount_max == Decimal("1000")
        assert market.price_min == Decimal("0.01")
        assert market.price_max == Decimal("1000000")
        assert market.cost_min == Decimal("10")
        assert market.cost_max == Decimal("100000")
        assert market.market_min == Decimal("5")
        assert market.market_max == Decimal("50000")


class TestMarketParser:
    def test_parser_keeps_missing_limits_as_none(self) -> None:
        parsed = MARKET_PARSER.search(
            {
                "symbol": "BTC/USDT",
                "base": "BTC",
                "quote": "USDT",
                "limits": {
                    "amount": {"min": None, "max": None},
                    "price": {"min": None, "max": None},
                    "cost": {"min": None, "max": None},
                    "market": {"min": None, "max": None},
                },
            }
        )

        market = DecimalConverter(prefer_attrib_converters=True).structure(parsed, MarketResponse)

        assert market.raw_symbol == "BTC/USDT"
        assert market.amount_min is None
        assert market.price_max is None
        assert market.cost_min is None
        assert market.market_max is None

    def test_parser_structures_numeric_limits_as_decimals(self) -> None:
        parsed = MARKET_PARSER.search(
            {
                "symbol": "ETH/USDT",
                "base": "ETH",
                "quote": "USDT",
                "limits": {
                    "amount": {"min": 0.001, "max": 10},
                    "price": {"min": "0.01", "max": "5000"},
                    "cost": {"min": "5", "max": "50000"},
                    "market": {"min": "1", "max": "100"},
                },
            }
        )

        market = DecimalConverter(prefer_attrib_converters=True).structure(parsed, MarketResponse)

        assert market.amount_min == Decimal("0.001")
        assert market.amount_max == Decimal("10")
        assert market.price_min == Decimal("0.01")
        assert market.price_max == Decimal("5000")
        assert market.cost_min == Decimal("5")
        assert market.cost_max == Decimal("50000")
        assert market.market_min == Decimal("1")
        assert market.market_max == Decimal("100")


class TestBinanceClientOhlcv:
    def test_get_ohlcv_uses_daily_urls_and_chunks_results(
        self, client: BinanceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fetched_urls: list[str] = []
        processed_heights: list[int] = []

        def fetch(url: str) -> pl.DataFrame:
            fetched_urls.append(url)
            return _raw_ohlcv_row()

        def process(ohlcv: pl.DataFrame, resolution: int, symbol: str) -> pl.DataFrame:
            processed_heights.append(ohlcv.height)
            assert resolution == 60
            assert symbol == "BTCUSDT"
            return pl.DataFrame({"rows": [ohlcv.height]})

        monkeypatch.setattr(client, "_fetch", fetch)
        monkeypatch.setattr(client, "_process_ohlcv", process)

        result = list(
            client.get_ohlcv(
                "BTC",
                "USDT",
                ExchangeClientResolution(seconds=60, name="1m"),
                date(2024, 5, 25),
                date(2024, 5, 26),
                min_chunk_size=2,
            )
        )

        assert fetched_urls == [
            "https://data.bafrapy-test.com/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-05-25.zip",
            "https://data.bafrapy-test.com/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-05-26.zip",
        ]
        assert processed_heights == [2]
        assert result[0].to_dict(as_series=False) == {"rows": [2]}

    def test_get_ohlcv_uses_monthly_urls_for_large_ranges(
        self, client: BinanceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fetched_urls: list[str] = []

        def fetch(url: str) -> pl.DataFrame:
            fetched_urls.append(url)
            return _raw_ohlcv_row()

        monkeypatch.setattr(client, "_fetch", fetch)
        monkeypatch.setattr(client, "_process_ohlcv", lambda ohlcv, resolution, symbol: ohlcv)

        list(
            client.get_ohlcv(
                "BTC",
                "USDT",
                ExchangeClientResolution(seconds=60, name="1m"),
                date(2024, 5, 1),
                date(2024, 6, 1),
            )
        )

        assert fetched_urls == [
            "https://data.bafrapy-test.com/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-05.zip",
            "https://data.bafrapy-test.com/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-06.zip",
        ]

    def test_process_ohlcv_normalizes_values_and_adds_missing_candles(self, client: BinanceClient) -> None:
        raw = pl.DataFrame(
            {
                "time": ["1748131200000000", "1748131320000000"],
                "open": ["1.1000", "1.2000"],
                "high": ["1.2000", "1.3000"],
                "low": ["1.0000", "1.1000"],
                "close": ["1.1500", "1.2500"],
                "volume": ["1800.000", "900.00"],
                "quote_volume": ["2000.00", "1080.000"],
            }
        )

        result = client._process_ohlcv(raw, 60, "BTCUSDT")

        assert result.height == 3
        assert result["exchange"].to_list() == ["binance", "binance", "binance"]
        assert result["symbol"].to_list() == ["BTCUSDT", "BTCUSDT", "BTCUSDT"]
        assert result["resolution"].to_list() == [60, 60, 60]
        for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
            assert result.schema[col] == pl.Decimal(precision=38, scale=0)
        assert result["open"].to_list()[0] == Decimal("11000")
        assert result["quote_volume"].to_list()[0] == Decimal("20000000")
        assert result["base_decimals"].to_list()[0] == 3
        assert result["quote_decimals"].to_list()[0] == 4
        assert result["time"].dt.strftime("%Y-%m-%d %H:%M:%S").to_list() == [
            "2025-05-25 00:00:00",
            "2025-05-25 00:01:00",
            "2025-05-25 00:02:00",
        ]
