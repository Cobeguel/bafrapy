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
    base_decimals: int = field(validator=validators.instance_of(int))
    quote_decimals: int = field(validator=validators.instance_of(int))
    timestamp: datetime = field(validator=validators.instance_of(datetime))
    open: int = field(validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    high: int = field(validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    low: int = field(validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    close: int = field(validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    volume: int = field(default=0, validator=validators.and_(validators.instance_of(int), validators.ge(0)))
    quote_volume: int = field(default=0, validator=validators.and_(validators.instance_of(int), validators.ge(0)))

    def _assert_is_valid_ohlcv(self) -> None:
        Normalizer.assert_decimals(self.base_decimals)
        Normalizer.assert_decimals(self.quote_decimals)
        if self.high < self.low or self.close < self.low or self.close > self.high:
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
        base_decimals = Normalizer.decimal_places(open)
        if any(Normalizer.decimal_places(value) != base_decimals for value in (high, low, close)):
            raise ValueError("Base values must use the same decimals")
        if volume is None:
            volume = Decimal(0)
        elif Normalizer.decimal_places(volume) != base_decimals:
            raise ValueError("Base values must use the same decimals")

        if quote_volume is None:
            quote_decimals = 0
            quote_volume = Decimal(0)
        else:
            quote_decimals = Normalizer.decimal_places(quote_volume)

        return cls(
            pair=pair,
            resolution=resolution,
            base_decimals=base_decimals,
            quote_decimals=quote_decimals,
            timestamp=timestamp,
            open=Normalizer.normalize_decimal(open, base_decimals),
            high=Normalizer.normalize_decimal(high, base_decimals),
            low=Normalizer.normalize_decimal(low, base_decimals),
            close=Normalizer.normalize_decimal(close, base_decimals),
            volume=Normalizer.normalize_decimal(volume, base_decimals),
            quote_volume=Normalizer.normalize_decimal(quote_volume, quote_decimals),
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
        values = (open, high, low, close, volume, quote_volume)
        if not all(isinstance(value, float) for value in values):
            unsupported = next(value for value in values if not isinstance(value, float))
            raise TypeError(f"Unsupported value type: {type(unsupported)}")

        return cls.from_decimal(
            pair=pair,
            resolution=resolution,
            timestamp=timestamp,
            open=Decimal(str(open)),
            high=Decimal(str(high)),
            low=Decimal(str(low)),
            close=Decimal(str(close)),
            volume=Decimal(str(volume)),
            quote_volume=Decimal(str(quote_volume)),
        )

    @property
    def base(self) -> Currency:
        return self.pair.base

    @property
    def quote(self) -> Currency:
        return self.pair.quote

    @property
    def open_emoney(self) -> EMoney:
        return EMoney(value=self.open, currency=self.base, decimals=self.base_decimals)

    @property
    def high_emoney(self) -> EMoney:
        return EMoney(value=self.high, currency=self.base, decimals=self.base_decimals)

    @property
    def low_emoney(self) -> EMoney:
        return EMoney(value=self.low, currency=self.base, decimals=self.base_decimals)

    @property
    def close_emoney(self) -> EMoney:
        return EMoney(value=self.close, currency=self.base, decimals=self.base_decimals)

    @property
    def volume_emoney(self) -> EMoney:
        return EMoney(value=self.volume, currency=self.base, decimals=self.base_decimals)

    @property
    def quote_volume_emoney(self) -> EMoney:
        return EMoney(value=self.quote_volume, currency=self.quote, decimals=self.quote_decimals)

    def decimal_open(self) -> Decimal:
        return Normalizer.to_decimal(self.open, self.base_decimals)

    def decimal_high(self) -> Decimal:
        return Normalizer.to_decimal(self.high, self.base_decimals)

    def decimal_low(self) -> Decimal:
        return Normalizer.to_decimal(self.low, self.base_decimals)

    def decimal_close(self) -> Decimal:
        return Normalizer.to_decimal(self.close, self.base_decimals)

    def decimal_volume(self) -> Decimal:
        return Normalizer.to_decimal(self.volume, self.base_decimals)

    def decimal_quote_volume(self) -> Decimal:
        return Normalizer.to_decimal(self.quote_volume, self.quote_decimals)

    def float_open(self) -> float:
        return Normalizer.to_float(self.open, self.base_decimals)

    def float_high(self) -> float:
        return Normalizer.to_float(self.high, self.base_decimals)

    def float_low(self) -> float:
        return Normalizer.to_float(self.low, self.base_decimals)

    def float_close(self) -> float:
        return Normalizer.to_float(self.close, self.base_decimals)

    def float_volume(self) -> float:
        return Normalizer.to_float(self.volume, self.base_decimals)

    def float_quote_volume(self) -> float:
        return Normalizer.to_float(self.quote_volume, self.quote_decimals)
