from typing import List, Optional

import polars as pl

import uuid

from adbc_driver_gizmosql import dbapi
from adbc_driver_gizmosql.dbapi import Connection
from attrs import define, field

from bafrapy.datawarehouse.base import (
    HistoricalRange,
    Market,
    OHLCVRepository,
)

from datetime import date

from typing import Iterator


class DucklakeError(Exception):
    pass


@define
class DucklakeClientBuilder:
    host: str
    port: int
    username: str
    password: str
    tls: bool = True
    tls_skip_verify: bool = False

    def _build_uri(self) -> str:
        scheme = "grpc+tls" if self.tls else "grpc"
        return f"{scheme}://{self.host}:{self.port}"

    def build(self) -> Connection:
        return dbapi.connect(
            self._build_uri(),
            username=self.username,
            password=self.password,
            tls_skip_verify=self.tls_skip_verify,
        )


OHLCV_COLUMNS = [
    "time",
    "exchange",
    "symbol",
    "resolution",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "quote_decimals",
    "base_decimals",
    "generated",
]


@define
class DucklakeOHLCVRepository(OHLCVRepository):
    _client: Connection = field(alias="client")
    _database: str = field(alias="database")
    _schema: str = field(alias="schema")

    def _ohlcv_table_name(self) -> str:
        return f"{self._database}.{self._schema}.crypto_ohlcv"

    def __del__(self) -> None:
        client = getattr(self, "_client", None)
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def _execute(self, query: str):
        try:
            return self._client.execute(query)
        except Exception as exc:
            raise DucklakeError("Error executing query") from exc

    def list_exchanges(self) -> List[str]:
        q = f"SELECT DISTINCT exchange FROM {self._ohlcv_table_name()} ORDER BY exchange"
        return self._execute(q).fetch_arrow_table().column("exchange").to_pylist()

    def list_symbols(self, exchange: str) -> List[str]:
        q = f"""
            SELECT DISTINCT symbol
            FROM {self._ohlcv_table_name()}
            WHERE exchange = '{exchange}'
            ORDER BY symbol
        """
        return self._execute(q).fetch_arrow_table().column("symbol").to_pylist()

    def market_historical_range(self, exchange: str, symbol: str, resolution: int) -> Optional[HistoricalRange]:
        q = f"""
            SELECT MIN(time) AS start_time, MAX(time) AS end_time
            FROM {self._ohlcv_table_name()}
            WHERE exchange = '{exchange}'
                AND symbol = '{symbol}'
                AND resolution = {resolution}
        """
        table = self._execute(q).fetch_arrow_table()
        start = table.column("start_time")[0].as_py()
        end = table.column("end_time")[0].as_py()
        if start is None or end is None:
            return None

        return HistoricalRange(
            market=Market(exchange=exchange, symbol=symbol, base="", quote=""),
            start=start,
            end=end,
        )

    def count_rows(self) -> int:
        q = f"SELECT COUNT(*) AS n FROM {self._ohlcv_table_name()}"
        return int(self._execute(q).fetch_arrow_table().column("n")[0].as_py())

    def insert_ohlcv(self, data: pl.DataFrame):
        if data.is_empty():
            return True

        if set(data.columns) != set(OHLCV_COLUMNS):
            raise DucklakeError(f"Invalid OHLCV columns. Expected {OHLCV_COLUMNS}, got {list(data.columns)}")

        data = data.unique(subset=["time", "exchange", "symbol", "resolution"])

        # data = data.select(OHLCV_COLUMNS).with_columns(
        #     [
        #         pl.col(c).cast(pl.Decimal(precision=38, scale=0))
        #         for c in ["open", "high", "low", "close", "volume", "quote_volume"]
        #     ]
        # )

        with self._client.cursor() as cursor:
            table_name = f"staging_{uuid.uuid4().hex}"
            try:
                cursor.adbc_ingest(
                    table_name=table_name,
                    data=data.to_arrow(),
                    mode="replace",
                    temporary=True,
                )

                cursor.execute(
                    f"""
                    MERGE INTO {self._ohlcv_table_name()} AS t
                    USING {table_name} AS s
                    ON  t.time = s.time
                    AND t.exchange = s.exchange
                    AND t.symbol = s.symbol
                    AND t.resolution = s.resolution
                    WHEN NOT MATCHED THEN INSERT BY NAME
                    """
                )
            except Exception as exc:
                raise DucklakeError("Error inserting OHLCV") from exc
            finally:
                cursor.close()

    def get_ohlcv(self, exchange: str, symbol: str, resolution: int, start: date, end: date) -> pl.DataFrame:
        q = f"""
            SELECT * 
            FROM {self._ohlcv_table_name()} 
            WHERE exchange = '{exchange}' 
                AND symbol = '{symbol}' 
                AND resolution = {resolution} 
                AND time BETWEEN '{start}' AND '{end}'
            ORDER BY time ASC
        """
        return pl.from_arrow(self._execute(q).fetch_arrow_table())

    def get_ohlcv_stream(
        self, exchange: str, symbol: str, resolution: int, start: date, end: date, chunk_size: int = 100_000
    ) -> Iterator[pl.DataFrame]:
        if chunk_size <= 0:
            raise DucklakeError("chunk_size must be greater than 0")

        last_time = None
        more_data = True

        while more_data:
            if last_time is None:
                current_time = f"time >= '{start}'"
            else:
                current_time = f"time > '{last_time}'"

            q = f"""
                SELECT *
                FROM {self._ohlcv_table_name()}
                WHERE exchange = '{exchange}'
                    AND symbol = '{symbol}'
                    AND resolution = {resolution}
                    AND {current_time}
                    AND time <= '{end}'
                ORDER BY time
                LIMIT {chunk_size}
            """

            try:
                batch = pl.from_arrow(self._execute(q).fetch_arrow_table())
            except Exception as exc:
                raise DucklakeError("Error streaming OHLCV") from exc

            more_data = not batch.is_empty() and batch.height == chunk_size

            if not batch.is_empty():
                yield batch
                last_time = batch["time"][-1]


@define
class DucklakeOHLCVRepositoryBuilder:
    host: str
    port: int
    username: str
    password: str
    database: str
    schema: str
    tls: bool = False
    tls_skip_verify: bool = False

    def _build_uri(self) -> str:
        scheme = "grpc+tls" if self.tls else "grpc"
        return f"{scheme}://{self.host}:{self.port}"

    def build(self) -> DucklakeOHLCVRepository:
        return DucklakeOHLCVRepository(
            client=dbapi.connect(
                self._build_uri(),
                username=self.username,
                password=self.password,
                tls_skip_verify=self.tls_skip_verify,
            ),
            database=self.database,
            schema=self.schema,
        )
