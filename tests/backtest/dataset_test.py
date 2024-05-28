import unittest

from datetime import datetime, timedelta

import pandas as pd

import bafrapy.backtest.dataset as dataset


class TestDataSetPandas(unittest.TestCase):
    def setUpFixed(self, candles):
        candles = 5
        start_date = datetime(2024, 1, 1)
        dates = [start_date + timedelta(days=x) for x in range(candles)]
        data = {
            'Date': dates,
            'Open': [2]*candles,
            'High': [3]*candles,
            'Low': [1]*candles,
            'Close': [2]*candles,
            'Volume': [1000]*candles
        }
        df = pd.DataFrame(data)
        df['Date'] = pd.to_datetime(df['Date'])
        self.dataset = dataset.DataSetPandas(86400, data=df)

    def test_next_data_fixed(self):
        candles = 5
        self.setUpFixed(candles)
        for i in range(candles):
            ohlcv = self.dataset.next_data()
            self.assertIsNotNone(ohlcv)
            self.assertEqual(ohlcv.open, 2)
            self.assertEqual(ohlcv.high, 3)
            self.assertEqual(ohlcv.low, 1)
            self.assertEqual(ohlcv.close, 2)
            self.assertEqual(ohlcv.volume, 1000)

        self.assertIsNone(self.dataset.next_data())

