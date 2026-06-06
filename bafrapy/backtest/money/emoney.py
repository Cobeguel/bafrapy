from decimal import ROUND_HALF_EVEN, Decimal

from attrs import define, field, validators

from bafrapy.backtest.money.currency import Currency


@define(frozen=True, slots=True)
class Normalizer:
    @staticmethod
    def assert_decimals(decimals: int) -> None:
        if not isinstance(decimals, int) or isinstance(decimals, bool):
            raise TypeError(f"Unsupported decimals type: {type(decimals)}")
        if decimals < 0:
            raise ValueError(f"Decimals must be greater than 0: {decimals}")

    @staticmethod
    def decimal_places(value: Decimal) -> int:
        if not isinstance(value, Decimal):
            raise TypeError(f"Unsupported value type: {type(value)}")
        exponent = value.as_tuple().exponent
        return -exponent if exponent < 0 else 0

    @staticmethod
    def normalize_decimal(value: Decimal, decimals: int) -> int:
        Normalizer.assert_decimals(decimals)
        if not isinstance(value, Decimal):
            raise TypeError(f"Unsupported value type: {type(value)}")

        scaled = (value * (Decimal(10) ** decimals)).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_EVEN,
        )
        return int(scaled)

    @staticmethod
    def normalize_float(value: float, decimals: int) -> int:
        Normalizer.assert_decimals(decimals)
        if not isinstance(value, float):
            raise TypeError(f"Unsupported value type: {type(value)}")

        return Normalizer.normalize_decimal(Decimal(str(value)), decimals)

    @staticmethod
    def to_decimal(value: int, decimals: int) -> Decimal:
        Normalizer.assert_decimals(decimals)
        if not isinstance(value, int):
            raise TypeError(f"Unsupported value type: {type(value)}")

        return Decimal(value) / (Decimal(10) ** decimals)

    @staticmethod
    def to_float(value: int, decimals: int) -> float:
        Normalizer.assert_decimals(decimals)
        if not isinstance(value, int):
            raise TypeError(f"Unsupported value type: {type(value)}")

        return float(Normalizer.to_decimal(value, decimals))


@define(frozen=True, slots=True)
class EMoney:
    value: int = field(validator=validators.instance_of(int))
    currency: Currency = field(validator=validators.instance_of(Currency))
    decimals: int = field(validator=validators.instance_of(int))

    def __attrs_post_init__(self) -> None:
        Normalizer.assert_decimals(self.decimals)

    def _assert_is_valid_emoney(self, m: "EMoney") -> None:
        if not isinstance(m, EMoney):
            raise TypeError(f"Unsupported type: {type(m)}")
        if self.currency != m.currency:
            raise TypeError(f"Cannot operate on assets with different currencies: {self.currency} != {m.currency}")

    def _assert_is_number(self, n: int | float | Decimal) -> None:
        if not isinstance(n, (int, float, Decimal)) or isinstance(n, bool):
            raise TypeError(f"Unsupported number type: {type(n)}")

    def _is_integer(self, other: object) -> bool:
        return isinstance(other, int) and not isinstance(other, bool)

    def _aligned_values(self, m: "EMoney") -> tuple[int, int, int]:
        self._assert_is_valid_emoney(m)
        decimals = max(self.decimals, m.decimals)
        return (
            self.value * (10 ** (decimals - self.decimals)),
            m.value * (10 ** (decimals - m.decimals)),
            decimals,
        )

    @classmethod
    def zero(cls, currency: Currency, decimals: int = 0) -> "EMoney":
        return cls(value=0, currency=currency, decimals=decimals)

    @classmethod
    def from_decimal(cls, amount: Decimal, currency: Currency) -> "EMoney":
        if not isinstance(amount, Decimal):
            raise TypeError(f"Unsupported amount type: {type(amount)}")
        if not isinstance(currency, Currency):
            raise TypeError(f"Unsupported currency type: {type(currency)}")

        decimals = Normalizer.decimal_places(amount)
        return cls(
            value=Normalizer.normalize_decimal(amount, decimals),
            currency=currency,
            decimals=decimals,
        )

    @classmethod
    def from_float(cls, value: float, currency: Currency) -> "EMoney":
        if not isinstance(value, float) or isinstance(value, bool):
            raise TypeError(f"Unsupported value type: {type(value)}")
        return cls.from_decimal(Decimal(str(value)), currency)

    def to_decimal(self) -> Decimal:
        return Normalizer.to_decimal(self.value, self.decimals)

    def to_float(self) -> float:
        return Normalizer.to_float(self.value, self.decimals)

    def is_negative(self) -> bool:
        return self.value < 0

    def is_positive(self) -> bool:
        return self.value > 0

    def is_zero(self) -> bool:
        return self.value == 0

    def __eq__(self, other: "EMoney | int") -> bool:
        if self._is_integer(other):
            return self.value == other
        self_value, other_value, _ = self._aligned_values(other)
        return self_value == other_value

    def __lt__(self, other: "EMoney | int") -> bool:
        if self._is_integer(other):
            return self.value < other
        self_value, other_value, _ = self._aligned_values(other)
        return self_value < other_value

    def __le__(self, other: "EMoney | int") -> bool:
        if self._is_integer(other):
            return self.value <= other
        self_value, other_value, _ = self._aligned_values(other)
        return self_value <= other_value

    def __gt__(self, other: "EMoney | int") -> bool:
        if self._is_integer(other):
            return self.value > other
        self_value, other_value, _ = self._aligned_values(other)
        return self_value > other_value

    def __ge__(self, other: "EMoney | int") -> bool:
        if self._is_integer(other):
            return self.value >= other
        self_value, other_value, _ = self._aligned_values(other)
        return self_value >= other_value

    def __add__(self, m: "EMoney") -> "EMoney":
        self_value, m_value, decimals = self._aligned_values(m)
        return EMoney(
            value=self_value + m_value,
            currency=self.currency,
            decimals=decimals,
        )

    def __neg__(self) -> "EMoney":
        return EMoney(
            value=-self.value,
            currency=self.currency,
            decimals=self.decimals,
        )

    def __sub__(self, m: "EMoney") -> "EMoney":
        self_value, m_value, decimals = self._aligned_values(m)
        return EMoney(
            value=self_value - m_value,
            currency=self.currency,
            decimals=decimals,
        )

    def __mul__(self, multiplier: int | float | Decimal) -> "EMoney":
        self._assert_is_number(multiplier)

        if isinstance(multiplier, int):
            return EMoney(
                value=self.value * int(multiplier),
                currency=self.currency,
                decimals=self.decimals,
            )

        if isinstance(multiplier, (float, Decimal)):
            scaled = (Decimal(self.value) * Decimal(str(multiplier))).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN)
            return EMoney(value=int(scaled), currency=self.currency, decimals=self.decimals)

        raise ValueError(f"Unsupported multiplier type: {type(multiplier)}")

    def __rmul__(self, multiplier: int | float | Decimal) -> "EMoney":
        return self.__mul__(multiplier)

    def __truediv__(self, divisor: int | float | Decimal) -> "EMoney":
        self._assert_is_number(divisor)

        if isinstance(divisor, int):
            return EMoney(
                value=self.value // int(divisor),
                currency=self.currency,
                decimals=self.decimals,
            )

        if isinstance(divisor, (float, Decimal)):
            scaled = (Decimal(self.value) / Decimal(str(divisor))).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN)
            return EMoney(
                value=int(scaled),
                currency=self.currency,
                decimals=self.decimals,
            )

        raise ValueError(f"Unsupported divisor type: {type(divisor)}")
