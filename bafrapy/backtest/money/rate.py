from attrs import define, field, validators

from bafrapy.backtest.money.currency import Currency
from bafrapy.backtest.money.emoney import EMoney, Normalizer


@define(frozen=True, slots=True)
class ERate:
    source: Currency = field(validator=validators.instance_of(Currency))
    target: Currency = field(validator=validators.instance_of(Currency))

    def convert(
        self,
        amount: EMoney,
        rate: EMoney,
        target_decimals: int | None = None,
    ) -> EMoney:
        if not isinstance(amount, EMoney):
            raise TypeError(f"Unsupported amount type: {type(amount)}")
        if amount.currency != self.source:
            raise TypeError(f"Invalid amount currency: {amount.currency} != {self.source}")

        if not isinstance(rate, EMoney):
            raise TypeError(f"Unsupported rate type: {type(rate)}")
        if rate.currency != self.target:
            raise TypeError(f"Invalid rate currency: {rate.currency} != {self.target}")

        if target_decimals is None:
            target_decimals = rate.decimals

        Normalizer.assert_decimals(target_decimals)

        value = amount.value * rate.value * (10**target_decimals) // (10 ** (amount.decimals + rate.decimals))

        return EMoney(
            value=value,
            currency=self.target,
            decimals=target_decimals,
        )
