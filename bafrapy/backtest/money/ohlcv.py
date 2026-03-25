from datetime import datetime
from decimal import Decimal

from attrs import define, field, validators

from bafrapy.backtest.money import Currency, EMoney, Normalizer


@define(slots=True, frozen=True)
class Pair:
    base: Currency = field(validator=validators.instance_of(Currency))
    quote: Currency = field(validator=validators.instance_of(Currency))


@define(slots=True, frozen=True)
class OHLCV:
    pair: Pair = field(validator=validators.instance_of(Pair))
    resolution: int = field(validator=validators.and_(validators.instance_of(int), validators.gt(0)))
    _normalizer: Normalizer = field(factory=Normalizer, init=False)
    timestamp: datetime = field(validator=validators.instance_of(datetime))
    _open: int = field(alias="open", validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    _high: int = field(alias="high", validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    _low: int = field(alias="low", validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    _close: int = field(alias="close", validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    _volume: int = field(
        alias="volume", default=0, validator=validators.and_(validators.instance_of(int), validators.ge(0))
    )
    _quote_volume: int = field(
        alias="quote_volume", default=0, validator=validators.and_(validators.instance_of(int), validators.ge(0))
    )

    def _assert_is_valid_ohlcv(self) -> None:
        if self._high < self._low or self._close < self._low or self._close > self._high:
            raise ValueError(
                f"Invalid OHLCV values: {self.high} < {self.low}, {self.close} < {self.low}, {self.close} > {self.high}"
            )

    def __attrs_post_init__(self):
        self._assert_is_valid_ohlcv()

    @classmethod
    def from_decimal(
        cls,
        pair: Pair,
        resolution: int,
        timestamp: datetime,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal = Decimal("0.0"),
        quote_volume: Decimal = Decimal("0.0"),
    ) -> "OHLCV":
        return cls(
            pair=pair,
            resolution=resolution,
            timestamp=timestamp,
            open=Normalizer.normalize_decimal(open, pair.base.decimals),
            high=Normalizer.normalize_decimal(high, pair.base.decimals),
            low=Normalizer.normalize_decimal(low, pair.base.decimals),
            close=Normalizer.normalize_decimal(close, pair.base.decimals),
            volume=Normalizer.normalize_decimal(volume, pair.base.decimals),
            quote_volume=Normalizer.normalize_decimal(quote_volume, pair.quote.decimals),
        )

    @classmethod
    def from_float(
        cls,
        pair: Pair,
        resolution: int,
        timestamp: datetime,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float = 0.0,
        quote_volume: float = 0.0,
    ) -> "OHLCV":
        return cls(
            pair=pair,
            resolution=resolution,
            timestamp=timestamp,
            open=Normalizer.normalize_float(open, pair.base.decimals),
            high=Normalizer.normalize_float(high, pair.base.decimals),
            low=Normalizer.normalize_float(low, pair.base.decimals),
            close=Normalizer.normalize_float(close, pair.base.decimals),
            volume=Normalizer.normalize_float(volume, pair.base.decimals),
            quote_volume=Normalizer.normalize_float(quote_volume, pair.quote.decimals),
        )

    @property
    def base(self) -> Currency:
        return self.pair.base

    @property
    def quote(self) -> Currency:
        return self.pair.quote

    @property
    def open(self) -> EMoney:
        return EMoney(value=self._open, currency=self.base)

    @property
    def high(self) -> EMoney:
        return EMoney(value=self._high, currency=self.base)

    @property
    def low(self) -> EMoney:
        return EMoney(value=self._low, currency=self.base)

    @property
    def close(self) -> EMoney:
        return EMoney(value=self._close, currency=self.base)

    @property
    def volume(self) -> EMoney:
        return EMoney(value=self._volume, currency=self.base)

    @property
    def quote_volume(self) -> EMoney:
        return EMoney(value=self._quote_volume, currency=self.quote)

    def decimal_open(self) -> Decimal:
        return self._normalizer.to_decimal(self._open, self.base.decimals)

    def decimal_high(self) -> Decimal:
        return self._normalizer.to_decimal(self._high, self.base.decimals)

    def decimal_low(self) -> Decimal:
        return self._normalizer.to_decimal(self._low, self.base.decimals)

    def decimal_close(self) -> Decimal:
        return self._normalizer.to_decimal(self._close, self.base.decimals)

    def decimal_volume(self) -> Decimal:
        return self._normalizer.to_decimal(self._volume, self.base.decimals)

    def decimal_quote_volume(self) -> Decimal:
        return self._normalizer.to_decimal(self._quote_volume, self.quote.decimals)

    def float_open(self) -> float:
        return self._normalizer.to_float(self._open, self.base.decimals)

    def float_high(self) -> float:
        return self._normalizer.to_float(self._high, self.base.decimals)

    def float_low(self) -> float:
        return self._normalizer.to_float(self._low, self.base.decimals)

    def float_close(self) -> float:
        return self._normalizer.to_float(self._close, self.base.decimals)

    def float_volume(self) -> float:
        return self._normalizer.to_float(self._volume, self.base.decimals)

    def float_quote_volume(self) -> float:
        return self._normalizer.to_float(self._quote_volume, self.quote.decimals)
