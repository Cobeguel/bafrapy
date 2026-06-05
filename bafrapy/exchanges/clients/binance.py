from datetime import date, datetime, timedelta
from typing import Iterator

import ccxt
import fsspec
import polars as pl

from attrs import define, field
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from bafrapy.exchanges.client import ExchangeClientResolution
from bafrapy.exchanges.clients.parser import MARKET_PARSER, DecimalConverter
from bafrapy.exchanges.markets import MarketResponse
from bafrapy.libs.parsetime import parse_timestamp
from bafrapy.libs.rangetimes import days_between, months_between
from bafrapy.logger import LogField, LoguruLogger as log


def _is_retryable(exc: Exception) -> bool:
    code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    return code in (429, 500, 502, 503, 504)


@define(slots=False, kw_only=False)
class BinanceClient:
    _data_url: str = field(default="https://data.binance.vision")
    _exchange: ccxt.Exchange = field(factory=lambda: ccxt.binance({"enableRateLimit": True}))

    @property
    def exchange_name(self) -> str:
        return "binance"

    def _daily_url(self, symbol: str, resolution: str, d: date) -> str:
        return f"{self._data_url}/data/spot/daily/klines/{symbol}/{resolution}/{symbol}-{resolution}-{d:%Y-%m-%d}.zip"

    def _monthly_url(self, symbol: str, resolution: str, d: date) -> str:
        return f"{self._data_url}/data/spot/monthly/klines/{symbol}/{resolution}/{symbol}-{resolution}-{d:%Y-%m}.zip"

    def build_symbol(self, base: str, quote: str) -> str:
        return f"{base}{quote}"

    def _get_agg_trade_timestamp(self, symbol, params: dict) -> datetime:
        response = self._exchange.publicGetAggTrades({"symbol": symbol, **params})
        if not response:
            raise ValueError(f"No aggregate trades found for symbol {symbol}")

        return parse_timestamp(response[0]["T"])

    def get_markets(self) -> list[MarketResponse]:
        converter = DecimalConverter(prefer_attrib_converters=True)
        markets: list[MarketResponse] = []

        for market in self._exchange.load_markets().values():
            if market.get("type") != "spot" or not market.get("base") or not market.get("quote"):
                continue

            parsed = MARKET_PARSER.search(market)
            if not parsed or not parsed.get("base") or not parsed.get("quote"):
                continue

            markets.append(converter.structure(parsed, MarketResponse))

        return markets

    def get_first_market_date(self, base: str, quote: str) -> datetime:
        return self._get_agg_trade_timestamp(self.build_symbol(base, quote), {"fromId": 0, "limit": 1})

    def get_last_market_date(self, base: str, quote: str) -> datetime:
        return self._get_agg_trade_timestamp(self.build_symbol(base, quote), {"limit": 1})

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=2, max=30),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch(self, url: str) -> pl.DataFrame:
        log().info(f"Fetching {url}")
        files = fsspec.open_files(f"zip://*.csv::{url}")
        if len(files) != 1:
            raise ValueError(f"Expected exactly one CSV in ZIP, found {len(files)}")

        with files[0] as f:
            return pl.read_csv(
                f,
                has_header=False,
                columns=[0, 1, 2, 3, 4, 5, 7],
                new_columns=["time", "open", "high", "low", "close", "volume", "quote_volume"],
                infer_schema=False,
            )

    def _process_ohlcv(self, ohlcv: pl.DataFrame, resolution: int, symbol: str) -> pl.DataFrame:
        log().info(f"Processing {len(ohlcv)} rows", LogField("symbol", symbol))
        ohlcv = ohlcv.with_columns(
            pl.lit(False).alias("generated"),
            pl.lit(self.exchange_name).alias("exchange"),
            pl.lit(symbol).alias("symbol"),
            pl.lit(resolution).alias("resolution"),
            pl.col("time").map_elements(parse_timestamp, return_dtype=pl.Datetime),
        )

        price_cols = ["open", "high", "low", "close", "quote_volume"]

        row_quote_decimals = pl.max_horizontal(
            *[pl.col(c).str.split(".").list.get(1).str.len_chars() for c in price_cols]
        )

        ohlcv = ohlcv.with_columns(
            *[
                (
                    pl.col(c).str.split(".").list.get(0)
                    + pl.col(c).str.split(".").list.get(1).str.pad_end(row_quote_decimals, "0")
                ).cast(pl.Decimal(precision=38, scale=0))
                for c in price_cols
            ],
            (pl.col("volume").str.split(".").list.get(0) + pl.col("volume").str.split(".").list.get(1)).cast(
                pl.Decimal(precision=38, scale=0)
            ),
            row_quote_decimals.cast(pl.UInt8).alias("quote_decimals"),
            pl.col("volume").str.split(".").list.get(1).str.len_chars().cast(pl.UInt8).alias("base_decimals"),
        )

        ohlcv = (
            ohlcv.sort("time")
            .upsample("time", every=f"{resolution}s")
            .with_columns(pl.all().forward_fill())
            .with_columns(
                pl.col("volume", "quote_volume").fill_null(0),
                pl.col("generated").fill_null(True),
            )
        )
        return ohlcv

    def get_ohlcv(
        self, base: str, quote: str, resolution: ExchangeClientResolution, start: date, end: date, min_chunk_size=10000
    ) -> Iterator[pl.DataFrame]:
        yesterday = date.today() - timedelta(days=1)
        end_effective = min(end, yesterday)
        if (end_effective - start).days >= 30:
            urls = []

            monthly_end = date(end_effective.year, end_effective.month, 1)

            for m in months_between(start, monthly_end):
                if m < monthly_end:
                    urls.append(self._monthly_url(self.build_symbol(base, quote), resolution.name, m))

            # Daily URLs are only available for resolutions up to 1 day
            if resolution.seconds <= 86400:
                for d in days_between(monthly_end, end_effective):
                    urls.append(self._daily_url(self.build_symbol(base, quote), resolution.name, d))

        else:
            urls = []
            # Daily URLs are only available for resolutions up to 1 day
            if resolution.seconds <= 86400:
                for d in days_between(start, end_effective):
                    urls.append(self._daily_url(self.build_symbol(base, quote), resolution.name, d))

        ohlcv = pl.DataFrame()
        for url in urls:
            raw_df = self._fetch(url)
            if raw_df.is_empty():
                continue
            ohlcv = pl.concat([ohlcv, raw_df])

            if ohlcv.height >= min_chunk_size:
                yield self._process_ohlcv(ohlcv, resolution.seconds, f"{base}{quote}")
                ohlcv = pl.DataFrame()

        if not ohlcv.is_empty():
            yield self._process_ohlcv(ohlcv, resolution.seconds, f"{base}{quote}")
