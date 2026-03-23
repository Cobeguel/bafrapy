from decimal import ROUND_HALF_EVEN, Decimal

from attrs import define, field, validators

from bafrapy.backtest.money.currency import Currency


@define(frozen=True, slots=True)
class EMoney:
    value: int = field(validator=validators.instance_of(int))
    currency: Currency = field(validator=validators.instance_of(Currency))

    def _assert_is_valid_emoney(self, m: "EMoney") -> None:
        if not isinstance(m, EMoney):
            raise TypeError(f"Unsupported type: {type(m)}")
        if self.currency != m.currency:
            raise TypeError(
                f"Cannot operate on assets with different currencies: "
                f"{self.currency} != {m.currency}"
            )

    def _assert_is_number(self, n: int | float | Decimal) -> None:
        if not isinstance(n, (int, float, Decimal)) or isinstance(n, bool):
            raise TypeError(f"Unsupported number type: {type(n)}")

    @classmethod
    def zero(cls, currency: Currency) -> "EMoney":
        return cls(value=0, currency=currency)

    @classmethod
    def from_decimal(cls, amount: Decimal, currency: Currency) -> "EMoney":
        if not isinstance(amount, Decimal):
            raise TypeError(f"Unsupported amount type: {type(amount)}")
        if not isinstance(currency, Currency):
            raise TypeError(f"Unsupported currency type: {type(currency)}")
        scaled = (amount * (Decimal(10) ** currency.decimals)).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_EVEN,
        )
        return cls(value=int(scaled), currency=currency)

    @classmethod
    def from_float(cls, value: float, currency: Currency) -> "EMoney":
        return cls.from_decimal(Decimal(str(value)), currency)

    def to_decimal(self) -> Decimal:
        return Decimal(self.value) / (Decimal(10) ** self.currency.decimals)

    def to_float(self) -> float:
        self._assert_is_valid_emoney(self)
        return float(self.to_decimal())

    def is_negative(self) -> bool:
        return self.value < 0

    def is_positive(self) -> bool:
        return self.value > 0

    def is_zero(self) -> bool:
        return self.value == 0

    def __eq__(self, m: "EMoney") -> bool:
        self._assert_is_valid_emoney(m)
        return self.currency == m.currency and self.value == m.value

    def __lt__(self, m: "EMoney") -> bool:
        self._assert_is_valid_emoney(m)
        return self.value < m.value

    def __le__(self, m: "EMoney") -> bool:
        self._assert_is_valid_emoney(m)
        return self.value <= m.value

    def __gt__(self, m: "EMoney") -> bool:
        self._assert_is_valid_emoney(m)
        return self.value > m.value

    def __ge__(self, m: "EMoney") -> bool:
        self._assert_is_valid_emoney(m)
        return self.value >= m.value

    def __add__(self, m: "EMoney") -> "EMoney":
        self._assert_is_valid_emoney(m)
        return EMoney(
            value=self.value + m.value,
            currency=self.currency,
        )

    def __neg__(self) -> "EMoney":
        return EMoney(
            value=-self.value,
            currency=self.currency,
        )

    def __sub__(self, m: "EMoney") -> "EMoney":
        self._assert_is_valid_emoney(m)
        return EMoney(
            value=self.value - m.value,
            currency=self.currency,
        )

    def __mul__(self, multiplier: int | float | Decimal) -> "EMoney":
        self._assert_is_number(multiplier)

        if isinstance(multiplier, int):
            return EMoney(
                value=self.value * int(multiplier),
                currency=self.currency,
            )

        if isinstance(multiplier, (float, Decimal)):
            scaled = (Decimal(self.value) * Decimal(str(multiplier))).quantize(
                Decimal("1"), rounding=ROUND_HALF_EVEN
            )
            return EMoney(
                value=int(scaled),
                currency=self.currency,
            )

        raise ValueError(f"Unsupported multiplier type: {type(multiplier)}")

    def __rmul__(self, multiplier: int | float | Decimal) -> "EMoney":
        return self.__mul__(multiplier)

    def __truediv__(self, divisor: int | float | Decimal) -> "EMoney":
        self._assert_is_number(divisor)

        if isinstance(divisor, int):
            return EMoney(
                value=self.value // int(divisor),
                currency=self.currency,
            )

        if isinstance(divisor, (float, Decimal)):
            scaled = (Decimal(self.value) / Decimal(str(divisor))).quantize(
                Decimal("1"), rounding=ROUND_HALF_EVEN
            )
            return EMoney(
                value=int(scaled),
                currency=self.currency,
            )

        raise ValueError(f"Unsupported divisor type: {type(divisor)}")
