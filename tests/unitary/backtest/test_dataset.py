from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import polars as pl

from bafrapy.backtest.dataset import DucklakeDataSet, PandasDataSet, PolarsDataSet
from bafrapy.backtest.money import Currency, OHLCV, Pair

__pytest__ = False

PAIR = Pair(base=Currency("BTC"), quote=Currency("USD"))
RESOLUTION = 86400


def _ohlcv_frame(candles: int) -> pd.DataFrame:
    start_date = datetime(2024, 1, 1)
    dates = [start_date + timedelta(days=x) for x in range(candles)]
    return pd.DataFrame(
        {
            "time": dates,
            "resolution": [RESOLUTION] * candles,
            "open": [200] * candles,
            "high": [300] * candles,
            "low": [100] * candles,
            "close": [200] * candles,
            "volume": [100000] * candles,
            "quote_volume": [0] * candles,
            "base_decimals": [2] * candles,
            "quote_decimals": [0] * candles,
        }
    )


class TestPandasDataSet:
    def test_iterates_ohlcv(self):
        dataset = PandasDataSet(pair=PAIR, resolution=RESOLUTION, data=_ohlcv_frame(5))
        assert dataset.has_data()

        for _ in range(5):
            ohlcv = dataset.next_data()
            assert isinstance(ohlcv, OHLCV)
            assert ohlcv.open == 200
            assert ohlcv.high == 300
            assert ohlcv.low == 100
            assert ohlcv.close == 200
            assert ohlcv.volume == 100000

        assert dataset.next_data() is None
        assert not dataset.has_data()


class TestPolarsDataSet:
    def test_iterates_ohlcv(self):
        dataset = PolarsDataSet(
            pair=PAIR,
            resolution=RESOLUTION,
            data=pl.from_pandas(_ohlcv_frame(4)),
        )

        for _ in range(4):
            ohlcv = dataset.next_data()
            assert ohlcv is not None
            assert ohlcv.close == 200

        assert dataset.next_data() is None


class TestDucklakeDataSet:
    DUCKLAKE_PAIR = Pair(base=Currency("BTC"), quote=Currency("USDT"))

    def test_streams_chunks_into_ohlcv(self):
        chunk = pl.DataFrame(
            {
                "time": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
                "exchange": ["binance", "binance"],
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "resolution": [RESOLUTION, RESOLUTION],
                "open": [100, 110],
                "high": [120, 130],
                "low": [90, 100],
                "close": [110, 120],
                "volume": [1000, 1100],
                "quote_volume": [0, 0],
                "base_decimals": [2, 2],
                "quote_decimals": [0, 0],
                "generated": [False, False],
            }
        )
        repository = MagicMock()
        repository.get_ohlcv_stream.return_value = iter([chunk])

        dataset = DucklakeDataSet(
            pair=self.DUCKLAKE_PAIR,
            resolution=RESOLUTION,
            repository=repository,
            exchange="binance",
            start=datetime(2024, 1, 1).date(),
            end=datetime(2024, 1, 2).date(),
        )

        repository.get_ohlcv_stream.assert_called_once_with(
            "binance",
            "BTCUSDT",
            RESOLUTION,
            datetime(2024, 1, 1).date(),
            datetime(2024, 1, 2).date(),
            100_000,
        )

        first = dataset.next_data()
        second = dataset.next_data()
        assert first is not None and first.open == 100
        assert second is not None and second.open == 110
        assert dataset.next_data() is None
        assert not dataset.has_data()

    def test_empty_stream(self):
        repository = MagicMock()
        repository.get_ohlcv_stream.return_value = iter([])

        dataset = DucklakeDataSet(
            pair=self.DUCKLAKE_PAIR,
            resolution=RESOLUTION,
            repository=repository,
            exchange="binance",
            start=datetime(2024, 1, 1).date(),
            end=datetime(2024, 1, 2).date(),
        )

        assert dataset.next_data() is None
        assert not dataset.has_data()
