from decimal import Decimal

from attrs import define, field, validators


@define(slots=False, kw_only=False)
class MarketResponse:
    raw_symbol: str = field(validator=[validators.instance_of(str), validators.min_len(1)])
    base: str = field(validator=[validators.instance_of(str), validators.min_len(1)])
    quote: str = field(validator=[validators.instance_of(str), validators.min_len(1)])

    active: bool = field(default=True, validator=validators.instance_of(bool))
    price_min: Decimal | None = field(default=None, validator=validators.optional(validators.instance_of(Decimal)))
    price_max: Decimal | None = field(default=None, validator=validators.optional(validators.instance_of(Decimal)))
    amount_min: Decimal | None = field(default=None, validator=validators.optional(validators.instance_of(Decimal)))
    amount_max: Decimal | None = field(default=None, validator=validators.optional(validators.instance_of(Decimal)))
    market_min: Decimal | None = field(default=None, validator=validators.optional(validators.instance_of(Decimal)))
    market_max: Decimal | None = field(default=None, validator=validators.optional(validators.instance_of(Decimal)))
    cost_min: Decimal | None = field(default=None, validator=validators.optional(validators.instance_of(Decimal)))
    cost_max: Decimal | None = field(default=None, validator=validators.optional(validators.instance_of(Decimal)))

    @property
    def symbol(self) -> str:
        return f"{self.base}{self.quote}"
