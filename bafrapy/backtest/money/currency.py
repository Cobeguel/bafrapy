from attrs import define


@define(frozen=True, slots=True)
class Currency:
    symbol: str
    decimals: int


CURRENCIES = {
    "ADA": Currency("ADA", 6),
    "AVAX": Currency("AVAX", 18),
    "BCH": Currency("BCH", 8),
    "BNB": Currency("BNB", 18),
    "BTC": Currency("BTC", 8),
    "DAI": Currency("DAI", 18),
    "DOGE": Currency("DOGE", 8),
    "DOT": Currency("DOT", 10),
    "ETH": Currency("ETH", 18),
    "LINK": Currency("LINK", 18),
    "LTC": Currency("LTC", 8),
    "MATIC": Currency("MATIC", 18),
    "SHIB": Currency("SHIB", 18),
    "SOL": Currency("SOL", 9),
    "TON": Currency("TON", 9),
    "TRX": Currency("TRX", 6),
    "USDC": Currency("USDC", 6),
    "USDT": Currency("USDT", 6),
    "XLM": Currency("XLM", 7),
    "XRP": Currency("XRP", 6),
}
