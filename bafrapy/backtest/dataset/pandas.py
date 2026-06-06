import pandas as pd
from attrs import define, field

from bafrapy.backtest.dataset.base import DataSet
from bafrapy.backtest.money import OHLCV


@define(kw_only=True)
class PandasDataSet(DataSet):
    data: pd.DataFrame
    _row_index: int = field(default=0, init=False)

    def _row_to_ohlcv(self, row: dict) -> OHLCV:
        return OHLCV(
            pair=self.pair,
            resolution=int(row["resolution"]),
            base_decimals=int(row["base_decimals"]),
            quote_decimals=int(row["quote_decimals"]),
            timestamp=row["time"],
            open=int(row["open"]),
            high=int(row["high"]),
            low=int(row["low"]),
            close=int(row["close"]),
            volume=int(row["volume"]),
            quote_volume=int(row["quote_volume"]),
        )

    def next_data(self) -> OHLCV | None:
        if self._row_index >= len(self.data):
            return None
        row = self.data.iloc[self._row_index].to_dict()
        self._row_index += 1
        self.current_data = self._row_to_ohlcv(row)
        return self.current_data

    def has_data(self) -> bool:
        return self._row_index < len(self.data)
