from datetime import datetime, timedelta

import pandas as pd

import bafrapy.backtest.dataset as dataset

__pytest__ = False


class TestDataSetPandas:

    def setup_method(self):
        self._init_dataset(5)

    def _init_dataset(self, candles):
        start_date = datetime(2024, 1, 1)
        dates = [start_date + timedelta(days=x) for x in range(candles)]
        df = pd.DataFrame({
            'Date': dates,
            'Open': [2] * candles,
            'High': [3] * candles,
            'Low': [1] * candles,
            'Close': [2] * candles,
            'Volume': [1000] * candles,
        })
        df['Date'] = pd.to_datetime(df['Date'])
        self.dataset = dataset.DataSetPandas(86400, data=df)

    def test_next_data_fixed(self):
        for _ in range(5):
            ohlcv = self.dataset.next_data()
            assert ohlcv is not None
            assert ohlcv.open == 2
            assert ohlcv.high == 3
            assert ohlcv.low == 1
            assert ohlcv.close == 2
            assert ohlcv.volume == 1000

        assert self.dataset.next_data() is None
