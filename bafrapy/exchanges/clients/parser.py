from decimal import Decimal

from cattrs import Converter
from jmespath import compile

MARKET_PARSER = compile("""{
    raw_symbol: symbol,
    base: base,
    quote: quote,
    amount_min: limits.amount.min && to_string(limits.amount.min),
    amount_max: limits.amount.max && to_string(limits.amount.max),
    price_min: limits.price.min && to_string(limits.price.min),
    price_max: limits.price.max && to_string(limits.price.max),
    cost_min: limits.cost.min && to_string(limits.cost.min),
    cost_max: limits.cost.max && to_string(limits.cost.max),
    market_min: limits.market.min && to_string(limits.market.min),
    market_max: limits.market.max && to_string(limits.market.max)
}""")


class DecimalConverter(Converter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.register_structure_hook(Decimal, lambda v, _: Decimal(v))
