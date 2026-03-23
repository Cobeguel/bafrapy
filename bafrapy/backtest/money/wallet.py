from typing import List

from attrs import define, field
from beartype import beartype

from .currency import Currency
from .emoney import EMoney


@define
class Wallet:
    _currencies: dict[Currency, EMoney] = field(factory=dict, init=False)

    @beartype
    def add_currency(self, currency: Currency) -> None:
        if currency not in self._currencies:
            self._currencies[currency] = EMoney.zero(currency)

    @beartype
    def add_balance(self, m: EMoney) -> None:
        if m.currency not in self._currencies:
            self.add_currency(m.currency)
        self._currencies[m.currency] += m

    @beartype
    def subtract_balance(self, m: EMoney) -> None:
        if m.currency not in self._currencies:
            self.add_currency(m.currency)
        self._currencies[m.currency] -= m

    @beartype
    def get_balance(self, currency: Currency) -> EMoney:
        try:
            return self._currencies[currency]
        except KeyError:
            return EMoney.zero(currency)

    @property
    def currencies(self) -> List[Currency]:
        return list(self._currencies.keys())


class SpotWallet(Wallet):
    @beartype
    def add_balance(self, m: EMoney) -> None:
        current_balance = self.get_balance(m.currency)
        if current_balance < -m:
            raise ValueError(f"Insufficient balance for currency {m.currency}")
        super().add_balance(m)

    @beartype
    def subtract_balance(self, m: EMoney) -> None:
        current_balance = self.get_balance(m.currency)
        if current_balance < m:
            raise ValueError(f"Insufficient balance for currency {m.currency}")
        super().subtract_balance(m)
