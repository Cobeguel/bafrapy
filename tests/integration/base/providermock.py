from datetime import date, datetime
from decimal import Decimal
from typing import List

import pandas as pd

from bafrapy.providers.base import Provider, Resolution, Symbol


class ProviderMock(Provider):
    def list_available_symbols(self) -> List[Symbol]:
        return [
            Symbol(symbol="BTCUSDT", base="BTC", quote="USDT", min_lot=Decimal("0.0001")),
            Symbol(symbol="ETHUSDT", base="ETH", quote="USDT", min_lot=Decimal("0.01")),
        ]

    def symbol_first_date(self, symbol: str) -> datetime:
        return datetime(2020, 1, 1)

    def symbol_last_date(self, symbol: str) -> datetime:
        return datetime(2025, 1, 1)

    def get_day_data(self, symbol: str, day: date, resolution: Resolution) -> pd.DataFrame:
        return pd.DataFrame({
            "timestamp": [datetime.combine(day, datetime.min.time())],
            "open": [Decimal(100.00)],
            "high": [Decimal(105.00)],
            "low": [Decimal(95.00)],
            "close": [Decimal(102.00)],
            "volume": [Decimal(123.45)]
        })

    def get_month_data(self, symbol: str, month: date, resolution: Resolution) -> pd.DataFrame:
        return pd.DataFrame({
            "timestamp": [datetime.combine(month, datetime.min.time())],
            "open": [Decimal(100.00)],
            "high": [Decimal(110.00)],
            "low": [Decimal(90.00)],
            "close": [Decimal(105.00)],
            "volume": [Decimal(12345.67)]
        })
