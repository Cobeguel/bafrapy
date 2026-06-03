from attrs import define


@define(frozen=True, slots=True)
class Currency:
    symbol: str
