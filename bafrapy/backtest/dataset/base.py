from abc import ABC, abstractmethod

from attrs import define, field

from bafrapy.backtest.money import OHLCV, Pair


@define(kw_only=True)
class DataSet(ABC):
    pair: Pair
    resolution: int
    current_data: OHLCV | None = field(default=None, init=False)

    def get_current_data(self) -> OHLCV | None:
        return self.current_data

    @abstractmethod
    def next_data(self) -> OHLCV | None:
        pass

    @abstractmethod
    def has_data(self) -> bool:
        pass
