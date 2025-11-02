from datetime import date, timedelta
from importlib import resources
from numbers import Number
from typing import Iterator, List

import pandas as pd

from attrs import define, field
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.query import QueryResult
from pandas.tseries.frequencies import to_offset

from bafrapy.datawarehouse.base import (
    OHLCVRepository,
    RowsResolution,
    SymbolAvailability,
    SymbolStats,
)
from bafrapy.libs.datetime import normalize_mixed_timestamp
from bafrapy.logger.log import LogField, LoguruLogger as log


@define
class ClikhouseOHLCVRepository(OHLCVRepository):
    _client: Client = field(alias='client')

    def _validate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        columns = set(['provider', 'symbol', 'resolution', 'time', 'open', 'high', 'low', 'close', 'volume'])

        if not columns.issubset(data.columns):
            raise ValueError(f"The dataframe is missing the following columns: {columns - set(data.columns)}")
        
        if not pd.api.types.is_integer_dtype(data['resolution']):
            raise ValueError("Resolution column must be an integer.")
        
        if not pd.api.types.is_datetime64_any_dtype(data['time']):
            try:
                data['time'] = pd.to_datetime(data['time'].apply(normalize_mixed_timestamp), utc=True)
            except Exception:
                log().error("Error converting time column to datetime ")
                raise
        
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if not data[col].apply(lambda x: isinstance(x, Number)).all():
                raise ValueError(f"{col} column must contain numeric values.")

        if (data[numeric_cols] <= 0).any().any():
            raise ValueError("Resolution, open, high, low, close, and volume columns must be positive.")

        return data

    def _fill_gaps(self, data: pd.DataFrame) -> pd.DataFrame:
        if len(data) == 0:
            return pd.DataFrame()

        resolution_distinct = data['resolution'].nunique()
        if resolution_distinct != 1:
            raise ValueError(f"Resolution is not constant in dataset. {resolution_distinct} distinct values found. Cannot fill gaps.")

        resample_offset = to_offset(timedelta(seconds=int(data['resolution'].iloc[0])))

        data.set_index('time', inplace=True)
        resampled_data = data.resample(resample_offset).asfreq()
        resampled_data['provider'] = data['provider'].iloc[0]
        resampled_data['symbol'] = data['symbol'].iloc[0]
        resampled_data['resolution'] = data['resolution'].iloc[0]

        resampled_data['close'] = resampled_data['close'].ffill()
        resampled_data['open'] = resampled_data['open'].fillna(resampled_data['close'])
        resampled_data['high'] = resampled_data['high'].fillna(resampled_data['close'])
        resampled_data['low'] = resampled_data['low'].fillna(resampled_data['close'])
        resampled_data['volume'] = resampled_data['volume'].fillna(0)

        resampled_data['state'] = resampled_data['state'].fillna(self._GAP_STATE)

        resampled_data.reset_index(inplace=True)
        return resampled_data

    def initialize(self):
        with resources.open_text('bafrapy.datawarehouse', 'ch_schema.sql') as f:
            file = f.read()
            queries = file.split(';')
            if len(queries) == 0:
                self._client.command(file)
            for query in queries:
                if query.strip() == '' or query.strip().startswith('/*') or query.strip().startswith('--'):
                    continue
                log().debug(f"Running query: {query}")
                self._client.command(query)

    def _command_int(self, q: str, parameters: dict) -> None:
        try:
            result = self._client.command(q, parameters=parameters)
            if not isinstance(result, int):
                raise ValueError(f"Command integer result expected, got {type(result)}. Command: {q} with parameters: {parameters}")
            return result
        except Exception as e:
            log().error("Error executing command integer", LogField("query", q), LogField("parameters", parameters), LogField("error", e))
            raise

    def _query_df(self, q: str, parameters: dict) -> pd.DataFrame:
        try:
            return self._client.query_df(q, parameters=parameters)
        except Exception as e:
            log().error("Error executing query", LogField("query", q), LogField("parameters", parameters), LogField("error", e))
            raise
        
    def _query(self, q: str, parameters: dict) -> QueryResult:
        try:
            return self._client.query(q, parameters=parameters)
        except Exception as e:
            log().error("Error executing query", LogField("query", q), LogField("parameters", parameters), LogField("error", e))
            raise

    def symbol_stats(self, provider: str, symbol: str) -> SymbolStats:
        q = '''
            SELECT provider, symbol, resolution, count(*) AS `num rows` 
            FROM crypto_ohlcv 
            WHERE provider={provider:String} 
                AND symbol={symbol:String} 
            GROUP BY provider, symbol, resolution 
            ORDER BY provider, symbol, resolution
        '''
        parameters = {'provider': provider, 'symbol': symbol}

        result =  self._query(q, parameters)
        try:
            rows_resolution = []
            for row in result.result_rows:
                rows_resolution.append(RowsResolution(
                    resolution=row[2],
                    num_rows=row[3],
                ))

            return SymbolStats(
                provider=result.result_rows[0][0],
                symbol=result.result_rows[0][1],
                rows_resolutions=rows_resolution
            )
        except Exception as e:
            log().error("Cannot parse symbol information", LogField("provider", provider), LogField("symbol", symbol),LogField("error", e))
            raise
        
    def provider_symbols_stats(self, provider: str) -> List[SymbolStats]:
        q = '''
            SELECT provider, symbol, resolution, count(*) AS `num rows` 
            FROM crypto_ohlcv 
            WHERE provider={provider:String}
            GROUP BY provider, symbol, resolution
            ORDER BY provider, symbol, resolution
        '''
        parameters = {'provider': provider}

        result =  self._query_df(q, parameters)
        grouped = result.groupby(['provider', 'symbol'])

        results = []
        for (provider, symbol), data in grouped:
            resolution_stats = []
            for _, row in data.iterrows():
                resolution_stats.append(RowsResolution(
                    resolution=row['resolution'],
                    num_rows=row['num rows']
                ))
            results.append(SymbolStats(provider=provider, symbol=symbol, rows_resolutions=resolution_stats))

        return results

    def list_providers(self) -> List[str]:
        q = '''
            SELECT DISTINCT provider FROM crypto_ohlcv 
            ORDER BY provider
        '''
        return self._query_df(q, parameters={})['provider'].tolist()

    def list_symbols(self, provider: str) -> List[str]:
        q = '''
            SELECT DISTINCT symbol FROM crypto_ohlcv 
            WHERE provider={provider:String} 
            ORDER BY symbol
        '''
        parameters = {'provider': provider}
        return self._query_df(q, parameters)['symbol'].tolist()

    def get_by_range(self, provider: str, symbol: str, resolution: int, start: date, end: date) -> pd.DataFrame:
        return self._query_df(
            '''SELECT * FROM crypto_ohlcv 
                WHERE provider={provider:String} 
                    AND symbol={symbol:String} 
                    AND resolution={resolution:Int} 
                    AND time BETWEEN {start:Date} AND {end:Date} 
            ORDER BY time''',
            parameters={
                'provider': provider,
                'symbol': symbol,
                'resolution': resolution,
                'start': start,
                'end': end
            })
    
    def count_rows(self, provider: str = "", symbol: str = "", resolution: int = 0) -> int:
        q = 'SELECT count(*) FROM crypto_ohlcv'
        parameters = {}
        if provider is not None and provider != '':
            q += ' WHERE provider={provider:String}'
            parameters['provider'] = provider
        if symbol is not None and symbol != '':
            q += ' WHERE symbol={symbol:String}'
            parameters['symbol'] = symbol
        if resolution is not None and resolution != 0:
            q += ' WHERE resolution={resolution:Int}'
            parameters['resolution'] = resolution

        log().debug(f"Counting rows for query: {q} with parameters: {parameters}")
        return self._command_int(q, parameters)

    def symbol_availability(self, provider: str, symbol: str, resolution: int) -> SymbolAvailability:
        q = ''' 
            SELECT MIN(time), MAX(time) from crypto_ohlcv 
            WHERE provider={provider:String} 
                AND symbol={symbol:String} 
                AND resolution={resolution:Int}
        '''
        parameters = {
            'provider': provider,
            'symbol': symbol,
            'resolution': resolution
        }
        result = self._client.query(q, parameters)
        begin, end = None, None
        try:
            begin = result.result_rows[0][0]
            end = result.result_rows[0][1]
        except IndexError:
            return (None, None)
        return (begin, end)
    
    def insert_data(self, data: pd.DataFrame, fill_gaps: bool = False):
        if len(data) == 0:
            log().warning("Inserting empty dataframe into Clickhouse. Nothing to do")
            return
        
        try:
            data = self._validate_data(data)
        except Exception as e:
            log().error("Error validating data", LogField("error", e))
            raise

        data['state'] = self._ORIGINAL_STATE        
        resolution_value = data['resolution'].iloc[0]
        if pd.isna(resolution_value) or resolution_value < 1:
            raise ValueError("Invalid resolution value. Must be a positive integer.")
        if fill_gaps:
            data = self._fill_gaps(data)

        self._client.insert_df('crypto_ohlcv', data)

    def get_by_range_stream(self, provider: str, symbol: str, resolution: int, start: date, end: date) -> Iterator[pd.DataFrame]:
        q = '''
            SELECT * FROM crypto_ohlcv
            WHERE provider={provider:String}
                AND symbol={symbol:String}
                AND resolution={resolution:Int}
                AND time BETWEEN {start:Date} AND {end:Date}
            ORDER BY time
        '''
        parameters = {
            'provider': provider,
            'symbol': symbol,
            'resolution': resolution,
            'start': start,
            'end': end
        }
        settings = {
            'max_block_size': 2**30,            # default 2^16
            'preferred_block_size_bytes': 2**30 # default 2^20 
        }
        df_stream = self._client.query_df_stream(q, parameters, settings)

        with df_stream:
                for df in df_stream:
                    df.set_index(df.columns[0], inplace=True)
                    yield df

    def clean_or_optimize_provider(self, provider: str):
        symbols = self.list_symbols(provider)
        for symbol in symbols:
            self.clean_or_optimize_symbol(provider, symbol)

    def clean_or_optimize_symbol(self, provider: str, symbol: str):
        q = f'''
            OPTIMIZE TABLE crypto_ohlcv PARTITION tuple('{provider}', '{symbol}') FINAL DEDUPLICATE
        '''
        self._client.command(q)
