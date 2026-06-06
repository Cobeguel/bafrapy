from collections.abc import Iterator
from datetime import date
from itertools import chain

from attrs import define, field

from bafrapy.backtest.dataset.base import DataSet
from bafrapy.backtest.money import OHLCV
from bafrapy.datawarehouse.base import OHLCVRepository


@define(kw_only=True)
class DucklakeDataSet(DataSet):
    repository: OHLCVRepository
    exchange: str
    start: date
    end: date
    chunk_size: int = 100_000
    _ohlcv: Iterator[OHLCV] = field(init=False)
    _peek: OHLCV | None = field(default=None, init=False)

    def __attrs_post_init__(self) -> None:
        chunks = self.repository.get_ohlcv_stream(
            self.exchange,
            f"{self.pair.base.symbol}{self.pair.quote.symbol}",
            self.resolution,
            self.start,
            self.end,
            self.chunk_size,
        )
        rows = chain.from_iterable(chunk.iter_rows(named=True) for chunk in chunks if not chunk.is_empty())
        self._ohlcv = map(self._row_to_ohlcv, rows)

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
        if self._peek is not None:
            self.current_data = self._peek
            self._peek = None
            return self.current_data
        self.current_data = next(self._ohlcv, None)
        return self.current_data

    def has_data(self) -> bool:
        if self._peek is not None:
            return True
        self._peek = next(self._ohlcv, None)
        return self._peek is not None
