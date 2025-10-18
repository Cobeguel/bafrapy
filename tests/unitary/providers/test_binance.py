import io
import zipfile

from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest
import responses

from freezegun import freeze_time
from responses import matchers

from bafrapy.libs.datetime import normalize_mixed_timestamp
from bafrapy.logger.log import LoguruLogger as log
from bafrapy.providers.base import Resolution, HTTPClient, BackoffConfig
from bafrapy.providers.binance import BinanceProvider, BinanceConfig

kline_rows = {
    0: {
        "open_time": 1748131200000000,
        "open": 1.1000,
        "high": 1.2000,
        "low": 1.0000,
        "close": 1.1000,
        "volume": 1800,
        "close_time": 1748131259999999,
        "quote_asset_volume": 2000,
        "number_of_trades": 4,
        "taker_buy_base_asset_volume": 700,
        "taker_buy_quote_asset_volume": 800,
        "ignore": 0
    },
    1: {
        "open_time": 1748131260000000,
        "open": 1.1000,
        "high": 1.2000,
        "low": 0.9500,
        "close": 1.1500,
        "volume": 8200,
        "close_time": 1748131319999999,
        "quote_asset_volume": 9300,
        "number_of_trades": 16,
        "taker_buy_base_asset_volume": 1700,
        "taker_buy_quote_asset_volume": 2000,
        "ignore": 0
    },
    2: {
        "open_time": 1748131320000000,
        "open": 1.1000,
        "high": 1.2000,
        "low": 1.0000,
        "close": 1.1300,
        "volume": 5000,
        "close_time": 1748131379999999,
        "quote_asset_volume": 5700,
        "number_of_trades": 8,
        "taker_buy_base_asset_volume": 900,
        "taker_buy_quote_asset_volume": 1000,
        "ignore": 0
    },
    3: {
        "open_time": 1748131380000000,
        "open": 1.1000,
        "high": 1.2000,
        "low": 1.0000,
        "close": 1.1300,
        "volume": 20500,
        "close_time": 1748131439999999,
        "quote_asset_volume": 23000,
        "number_of_trades": 33,
        "taker_buy_base_asset_volume": 1700,
        "taker_buy_quote_asset_volume": 2000,
        "ignore": 0
    },
    4: {
        "open_time": 1748131440000000,
        "open": 1.1000,
        "high": 1.2000,
        "low": 1.0000,
        "close": 1.1300,
        "volume": 18500,
        "close_time": 1748131499999999,
        "quote_asset_volume": 21000,
        "number_of_trades": 27,
        "taker_buy_base_asset_volume": 10700,
        "taker_buy_quote_asset_volume": 12100,
        "ignore": 0
    }
}


class TestBinanceConfig:
    @pytest.fixture(autouse=True)
    def setup_config(self):
        config = BinanceConfig(
            api_base_domain="https://api.binance.com",
            exchange_info_endpoint="api/v3/exchangeInfo",
            aggTrades_endpoint="api/v3/aggTrades",
            api_backoff_config=BackoffConfig(
                timeout=10,
                max_tries=3,
                backoff_factor=1,
                giveup_codes=[404]
            ),
            data_vision_domain="https://data.binance.vision",
            data_daily_klines_URL="data/spot/daily/klines",
            data_monthly_klines_URL="data/spot/monthly/klines",
            data_backoff_config=BackoffConfig(
                timeout=10,
                max_tries=3,
                backoff_factor=1,
                giveup_codes=[404]
            ),
            last_day_gaps_attempts=5
        )

        self.config = config

    def test_data_daily_URL(self):
        assert str(self.config.data_daily_URL("BTCUSDT", "1m", 2024, 5, 25)) == "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-05-25.zip"

    def test_data_monthly_URL(self):
        assert str(self.config.data_monthly_URL("BTCUSDT", "1m", 2024, 5)) == "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-05.zip"

    def test_symbols_URL(self):
        assert str(self.config.symbols_URL()) == "https://api.binance.com/api/v3/exchangeInfo"

    def test_aggTrades_URL(self):
        assert str(self.config.aggTrades_URL()) == "https://api.binance.com/api/v3/aggTrades"


class TestBinanceProvider:
    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self):
        log().deactivate()

    @pytest.fixture(autouse=True)
    def setup_provider(self):
        provider_name = "test_provider_binance"
        config = BinanceConfig(
            api_base_domain="https://api.bafrapy-test.com",
            exchange_info_endpoint="api/v3/exchangeInfo",
            aggTrades_endpoint="api/v3/aggTrades",
            api_backoff_config=BackoffConfig(
                timeout=10,
                max_tries=3,
                backoff_factor=1,
                giveup_codes=[404]
            ),
            data_vision_domain="https://data.bafrapy-test.com",
            data_daily_klines_URL="data/spot/daily/klines",
            data_monthly_klines_URL="data/spot/monthly/klines",
            data_backoff_config=BackoffConfig(
                timeout=10,
                max_tries=3,
                backoff_factor=1,
                giveup_codes=[404]
            ),
            last_day_gaps_attempts=5
        )
        self.provider = BinanceProvider(provider_name, config)

   
    @responses.activate
    def test_list_available_symbols_empty_filters(self):
        responses.add(
            responses.GET, 
            "https://api.bafrapy-test.com/api/v3/exchangeInfo", 
            json={
                "symbols": [
                    {
                        "symbol": "BTCUSDT", 
                        "baseAsset": "BTC", 
                        "quoteAsset": "USDT",
                        "filters": []
                        
                    },
                    {
                        "symbol": "ETHUSDT", 
                        "baseAsset": "ETH", 
                        "quoteAsset": "USDT",
                        "filters": []
                        
                    }
                ]
            })
        symbols = self.provider.list_available_symbols()
        assert len(symbols) == 2
        assert symbols[0].symbol == "BTCUSDT"
        assert symbols[0].base == "BTC"
        assert symbols[0].quote == "USDT"
        assert symbols[0].min_lot == Decimal("0")
        assert symbols[0].max_lot == Decimal("0")
        assert symbols[0].lot_increment == Decimal("0")
        assert symbols[1].symbol == "ETHUSDT"
        assert symbols[1].base == "ETH"
        assert symbols[1].quote == "USDT"
        assert symbols[1].min_lot == Decimal("0")
        assert symbols[1].max_lot == Decimal("0")
        assert symbols[1].lot_increment == Decimal("0")


    @responses.activate
    def test_list_available_symbols(self):
        responses.add(
            responses.GET, 
            "https://api.bafrapy-test.com/api/v3/exchangeInfo", 
            json={
                "symbols": [
                    {
                        "symbol": "BTCUSDT", 
                        "baseAsset": "BTC", 
                        "quoteAsset": "USDT",
                        "filters": [
                            {
                                "filterType": "LOT_SIZE",
                                "minQty": "0.00000000",
                                "maxQty": "100000000",
                                "stepSize": "0.00000000"
                                }
                        ]
                    },
                    {
                        "symbol": "ETHUSDT", 
                        "baseAsset": "ETH", 
                        "quoteAsset": "USDT",
                        "filters": [
                            {
                                "filterType": "LOT_SIZE",
                                "minQty": "0.00000000",
                                "maxQty": "100000000",
                                "stepSize": "0.00000000"
                                }
                        ]
                    }
                ]
            })
        symbols = self.provider.list_available_symbols()
        assert len(symbols) == 2
        assert symbols[0].symbol == "BTCUSDT"
        assert symbols[0].base == "BTC"
        assert symbols[0].quote == "USDT"
        assert symbols[0].min_lot == Decimal("0.00000000")
        assert symbols[0].max_lot == Decimal("100000000")
        assert symbols[0].lot_increment == Decimal("0.00000000")
        assert symbols[1].symbol == "ETHUSDT"
        assert symbols[1].base == "ETH"
        assert symbols[1].quote == "USDT"
        assert symbols[1].min_lot == Decimal("0.00000000")
        assert symbols[1].max_lot == Decimal("100000000")
        assert symbols[1].lot_increment == Decimal("0.00000000")


    @freeze_time("2024-05-26")
    @responses.activate
    def test_symbol_first_date(self):
        responses.add(
            responses.GET, 
            "https://api.bafrapy-test.com/api/v3/aggTrades?symbol=BTCUSDC&fromId=0&limit=1",
            match=[
                matchers.query_param_matcher({
                "symbol": "BTCUSDC",
                "fromId": "0",
                "limit": "1"
            })
        ],
            json = [
                {
                    "a": 54319067,
                    "p": "20131.35000000",
                    "q": "0.00260000",
                    "f": 56830993,
                    "l": 56830993,
                    "T": 1662076800796,
                    "m": True,
                    "M": True
                }
            ]
        )

        responses.add(
            responses.HEAD,
            "https://data.bafrapy-test.com/data/spot/monthly/klines/BTCUSDC/1m/BTCUSDC-1m-2022-09.zip",
            status=200
        )

        assert self.provider.symbol_first_date("BTCUSDC") == datetime.fromtimestamp(1662076800796 / 1000)

    @freeze_time("2024-05-27")
    @responses.activate
    def test_symbol_last_date_same_day(self):
        responses.add(
            responses.GET, 
            "https://api.bafrapy-test.com/api/v3/aggTrades?symbol=BTCUSDC&limit=1",
            match=[
                matchers.query_param_matcher({
                "symbol": "BTCUSDC",
                "limit": "1"
            })
            ],
            json = [
                {
                    "T":  1716819554000,  # 2024-05-27T16:19:14
                }
            ]
        )

        responses.add(
            responses.HEAD,
            "https://data.bafrapy-test.com/data/spot/daily/klines/BTCUSDC/1m/BTCUSDC-1m-2024-05-26.zip",
            status=200
        )

        expected_date = datetime(2024, 5, 26)
        actual_date = self.provider.symbol_last_date("BTCUSDC")
        assert actual_date.date() == expected_date.date()

    @freeze_time("2024-05-27")
    @responses.activate
    def test_symbol_last_date_lag_day(self):
        responses.add(
            responses.GET, 
            "https://api.bafrapy-test.com/api/v3/aggTrades?symbol=BTCUSDC&limit=1",
            match=[
                matchers.query_param_matcher({
                "symbol": "BTCUSDC",
                "limit": "1"
            })
            ],
            json = [
                {
                    "T":  1716819554000,  # 2024-05-27T16:19:14
                }
            ]
        )

        responses.add(
            responses.HEAD,
            "https://data.bafrapy-test.com/data/spot/daily/klines/BTCUSDC/1m/BTCUSDC-1m-2024-05-26.zip",
            status=404
        )
        responses.add(
            responses.HEAD,
            "https://data.bafrapy-test.com/data/spot/daily/klines/BTCUSDC/1m/BTCUSDC-1m-2024-05-25.zip",
            status=200
        )

        expected_date = datetime(2024, 5, 25)
        actual_date = self.provider.symbol_last_date("BTCUSDC")
        assert actual_date.date() == expected_date.date()


    @responses.activate
    def test_download_day_data(self):
        csv_buffer = io.StringIO()
        df = pd.DataFrame.from_dict(kline_rows, orient="index")
        df.to_csv(csv_buffer, index=False, header=False)
        df.to_csv('test.csv', index=False, header=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("BAFRAPY-1m-2024-05-25.csv", csv_bytes)
        zip_buffer.seek(0)

        responses.add(
            responses.GET,
            "https://data.bafrapy-test.com/data/spot/daily/klines/BAFRAPY/1m/BAFRAPY-1m-2024-05-25.zip",
            body=zip_buffer.read(),
            status=200,
            content_type="application/zip"
        )

        df_result = self.provider.get_day_data("BAFRAPY", datetime(2024, 5, 25).date(), Resolution("1m", 60))

        assert isinstance(df_result, pd.DataFrame)
        df_result.to_csv('test2.csv', index=False, header=False)
        assert len(df_result) == 5
        for index, row in df_result.iterrows():
            assert row["open"] == kline_rows[index]["open"]
            assert row["high"] == kline_rows[index]["high"]
            assert row["low"] == kline_rows[index]["low"]
            assert row["close"] == kline_rows[index]["close"]
            assert row["volume"] == kline_rows[index]["volume"]
            assert row["time"].date() == normalize_mixed_timestamp(kline_rows[index]["open_time"]).date()
            assert row["resolution"] == 60
            assert row["provider"] == "test_provider_binance"