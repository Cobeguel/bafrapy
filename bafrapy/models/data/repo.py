from dataclasses import dataclass
from datetime import date
from typing import Iterator

import pandas as pd
import urllib3

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from loguru import logger

from bafrapy.env_reader.database import EnvReader
from bafrapy.libs.singleton import Singleton


@dataclass
class DataFields(metaclass=Singleton):
    symbol: str = None
    resolution: int = None
    time: date = None
    open: float = None
    high: float = None
    low: float = None
    close: float = None
    volume: float = None

@dataclass
class DataRepository(metaclass=Singleton):
    client: Client = None

    def __init__(self, pool: bool = False):
        logger.info("Initializing Clickhouse client")
        pool = None
        if pool:
            pool = urllib3.PoolManager(num_pools=8, maxsize=16, retries=3, timeout=urllib3.Timeout(connect=2.0, read=5.0))
        self.client = get_client(host='localhost',
                                username=EnvReader().CH_USER,
                                password=EnvReader().CH_PASSWORD,
                                database=EnvReader().CH_DATABASE,
                                pool_mgr=pool)
        
        with open('./bafrapy/models/data/ch_schema.sql', 'r') as f:
            file = f.read()
            queries = file.split(';')
            if len(queries) == 0:
                self.client.command(file)
            for query in queries:
                if query.strip() == '' or query.strip().startswith('/*') or query.strip().startswith('--'):
                    continue
                logger.debug(f"Running query: {query}")
                self.client.command(query)
        
        logger.info("Clickhouse client initialized")

    def list_symbols(self) -> pd.DataFrame:
        return self.client.query_df('SELECT symbol FROM crypto_ohlcv GROUP BY symbol ORDER BY symbol')

    def list_symbols_stats(self) -> pd.DataFrame:
        return self.client.query_df('SELECT symbol, count(*) AS `num rows` FROM crypto_ohlcv  GROUP BY symbol ORDER BY symbol')

    def list_available_symbols_data(self) -> pd.DataFrame:
        return self.client.query_df(
            "SELECT symbol AS Symbol, CAST(year AS VARCHAR) AS Year FROM new_crypto_ohlcv_view GROUP BY symbol, year ORDER BY symbol, year")

    def list_resolutions(self, symbol: str) -> pd.DataFrame:
        return self.client.query_df('SELECT resolution FROM crypto_ohlcv WHERE  symbol={symbol:String} GROUP BY resolution ORDER BY resolution',
                                    parameters={'symbol': symbol})

    def get_by_range(self, symbol: str, resolution: int, start: date, end: date) -> pd.DataFrame:
        return self.client.query_df(
            'SELECT * FROM crypto_ohlcv WHERE symbol={symbol:String} AND resolution={resolution:Int} AND time BETWEEN {start:Date} AND {end:Date} ORDER BY time',
            parameters={
                'symbol': symbol,
                'resolution': resolution,
                'start': start,
                'end': end
            })

    def get_gap_days(self, symbol: str, resolution: int, start: date, end: date) -> pd.DataFrame:
        return self.client.query_df('''
                                    SELECT toDate({start:Date}) + number AS fecha
                                    FROM numbers(toUInt64(dateDiff('day', toDate({start:Date}), toDate({end:Date})))+1)
                                    WHERE NOT toDate({start:Date}) + number IN 
                                    (
                                        SELECT DISTINCT toDate(time) 
                                        FROM crypto_ohlcv 
                                        WHERE symbol = {symbol:String} AND resolution = {resolution:Int}
                                    )
                                    ORDER BY fecha''',
                                    parameters={
                                        'symbol': symbol,
                                        'resolution': resolution,
                                        'start': start,
                                        'end': end
                                    })

    def get_data(self, symbol: str, resolution: int, start: date, end: date) -> pd.DataFrame:
        return self.client.query_df(
            'SELECT time as datetime, cast(open as Float64) as Open, cast(high as Float64) as High, cast(low as Float64) as Low, cast(low as Float64) as Close, cast(volume as Float64) as Volume FROM crypto_ohlcv WHERE symbol={symbol:String} AND resolution={resolution:Int} AND time BETWEEN {start:Date} AND {end:Date} ORDER BY time',
            parameters={
                'symbol': symbol,
                'resolution': resolution,
                'start': start,
                'end': end
            })

    def get_range(self, symbol: str, resolution: int) -> tuple:
        result = self.client.query('SELECT MIN(time), MAX(time) from crypto_ohlcv WHERE symbol={symbol:String} AND resolution={resolution:Int}',
                                   parameters={
                                       'symbol': symbol,
                                       'resolution': resolution
                                   })
        return (result.result_rows[0][0], result.result_rows[0][1])


    def count_rows(self, symbol: str):
        return self.client.query_df(self.client.create_query_context('SELECT count() FROM ohlcv WHERE symbol={symbol:String}'),
                                    parameters={'symbol': symbol})

    def insert_data(self, df: pd.DataFrame):
        return self.client.insert_df('crypto_ohlcv', df)

    def optimize_table(self):
        self.client.command("OPTIMIZE TABLE crypto_ohlcv FINAL")

    def optimize_partition(self, symbol: str):
        stmt = f"OPTIMIZE TABLE crypto_ohlcv PARTITION '{symbol}' FINAL"
        self.client.command(stmt)

    def get_data_stream(self, symbol: str, resolution: int, start: date, end: date)  -> Iterator[pd.DataFrame]:
        df_stream = self.client.query_df_stream('SELECT time as datetime, cast(open as Float64) as Open, cast(high as Float64) as High, cast(low as Float64) as Low, cast(low as Float64) as Close, cast(volume as Float64) as Volume FROM crypto_ohlcv WHERE symbol={symbol:String} AND resolution={resolution:Int} AND time BETWEEN {start:Date} AND {end:Date} ORDER BY time',
            parameters={
                'symbol': symbol,
                'resolution': resolution,
                'start': start,
                'end': end
            },
            settings={
                'max_block_size': 2**30, # default 2^16
                'preferred_block_size_bytes': 2**30 # default 2^20 
            })
        with df_stream:
                for df in df_stream:
                    df.set_index(df.columns[0], inplace=True)
                    yield df
