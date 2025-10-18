import io
import json
import zipfile

from datetime import date, datetime, timedelta
from decimal import Decimal
from http import HTTPMethod
from typing import Iterator, List

import pandas as pd
import requests

from attrs import define, field
from cattrs import GenConverter
from dateutil.relativedelta import relativedelta
from requests import HTTPError, Request
from yarl import URL

from bafrapy.libs.datetime import normalize_mixed_timestamp
from bafrapy.logger.log import LogField, LoguruLogger as log
from bafrapy.providers.base import (
    BackoffConfig,
    HTTPClient,
    Provider,
    ProviderFactory,
    Resolution,
    Symbol,
)


@define
class BinanceConfig():
    _api_domain: str = field(alias="api_base_domain")
    _exchange_info_endpoint: str = field(alias="exchange_info_endpoint")
    _aggTrades_endpoint: str = field(alias="aggTrades_endpoint")
    _api_backoff_config: BackoffConfig = field(alias="api_backoff_config")

    _data_vision_domain: str = field(alias="data_vision_domain")
    _data_daily_klines_URL: str = field(alias="data_daily_klines_URL")
    _data_monthly_klines_URL: str = field(alias="data_monthly_klines_URL")
    _data_backoff_config: BackoffConfig = field(alias="data_backoff_config")

    _last_day_gaps_attempts: int = field(alias="last_day_gaps_attempts")

    @property
    def last_day_gaps_attempts(self) -> int:
        return self._last_day_gaps_attempts

    def get_api_backoff(self) -> BackoffConfig:
        return BackoffConfig(
            timeout=self._api_backoff_config.timeout,
            max_tries=self._api_backoff_config.max_tries,
            backoff_factor=self._api_backoff_config.backoff_factor,
            giveup_codes=self._api_backoff_config.giveup_codes
        )

    def get_data_backoff(self) -> BackoffConfig:
        return BackoffConfig(
            timeout=self._data_backoff_config.timeout,
            max_tries=self._data_backoff_config.max_tries,
            backoff_factor=self._data_backoff_config.backoff_factor,
            giveup_codes=self._data_backoff_config.giveup_codes
        )

    def symbols_URL(self):
        return URL(self._api_domain) / self._exchange_info_endpoint
    
    def aggTrades_URL(self):
        return URL(self._api_domain) / self._aggTrades_endpoint 

    # Example https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-04-29.zip
    def data_monthly_URL(self, symbol: str, resolution: str, year: int, month: int):
        return URL(self._data_vision_domain) / self._data_monthly_klines_URL / symbol / resolution / f"{symbol}-{resolution}-{year}-{month:02d}.zip"
    
    def data_daily_URL(self, symbol: str, resolution: str, year: int, month: int, day: int):
        return URL(self._data_vision_domain) / self._data_daily_klines_URL / symbol / resolution / f"{symbol}-{resolution}-{year}-{month:02d}-{day:02d}.zip"
        


@define
class BinanceProvider(Provider):
    _provider_name: str = field(alias="provider_name")
    _config: BinanceConfig = field(alias="config")
    _api_http_client: HTTPClient = field(init=False)
    _data_http_client: HTTPClient = field(init=False)

    def __attrs_post_init__(self):
        self._api_http_client = HTTPClient(
            backoff_config=self._config.get_api_backoff()
        )
        self._data_http_client = HTTPClient(
            backoff_config=self._config.get_data_backoff()
        )
    
    def _process_CSV_OHLCV(self, bytes: bytes, symbol: str, resolution: int) -> pd.DataFrame:
        with io.BytesIO(bytes) as zip_bytes:
            with zipfile.ZipFile(zip_bytes) as zf:
                if len(zf.namelist()) != 1:
                    raise ValueError("Binance provider expected exactly one file in the ZIP archive")
                csv_file_name = zf.namelist()[0]
                if not csv_file_name.endswith('.csv'):
                    raise ValueError("Binance provider expected exactly one CSV file in the ZIP archive")
                
                with zf.open(csv_file_name) as csv_file:
                    df = pd.read_csv(csv_file, header=None)
                    df = df.iloc[:, :6]
                    df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
                    df['time'] = pd.to_datetime(df['time'].apply(normalize_mixed_timestamp), utc=True)
                    df['resolution'] = resolution
                    df['provider'] = self._provider_name
                    return df

    def list_available_symbols(self) -> List[Symbol]:
        log().info(f"List available symbols", LogField(key="provider", value=self._provider_name))
        response = self._api_http_client.request(self._config.symbols_URL(), HTTPMethod.GET)
        body = response.json()
        log().info(f"Fetched available symbols from {self._provider_name}", LogField(key="symbols", value=len(body['symbols'])))

        symbols = list()
        for item in body['symbols']:            
            min_lot = Decimal(0)
            max_lot = Decimal(0)
            lot_increment = Decimal(0)
            try:
                filters = item['filters']
                for filter in filters:
                    if filter['filterType'] == 'LOT_SIZE':
                        min_lot = Decimal(filter['minQty'])
                        max_lot = Decimal(filter['maxQty'])
                        lot_increment = Decimal(filter['stepSize'])
            except KeyError as e:
                log().warning(f"Not found filters. They will not be stored", 
                              LogField(key='provider', value=self._provider_name), 
                              LogField(key="symbol", value=item['symbol']))
                pass

            symbols.append(Symbol(item['symbol'], item['baseAsset'], item['quoteAsset'], min_lot, max_lot, lot_increment))

        return symbols

    def symbol_first_date(self, symbol: str) -> datetime:
        log().info(f"Getting first date", LogField(key="provider", value=self._provider_name), LogField(key="symbol", value=symbol))
        params = {'symbol': symbol, 'fromId': 0, 'limit': 1}
        response = self._api_http_client.request(self._config.aggTrades_URL(), 'GET', params=params)
        body = response.json()

        first_date = datetime.fromtimestamp(body[0]['T'] / 1000)

        log().debug("First date for symbol", LogField(key="symbol", value=symbol), LogField(key="date", value=first_date))
        return first_date

    def symbol_last_date(self, symbol: str) -> date:
        log().info(f"Getting last date", LogField(key="provider", value=self._provider_name), LogField(key="symbol", value=symbol))
        params = {'symbol': symbol, 'limit': 1}
        response = self._api_http_client.request(self._config.aggTrades_URL(), HTTPMethod.GET, params=params)

        body = response.json()
        last_date = datetime.fromtimestamp(body[0]['T'] / 1000)
        if last_date.date() == date.today():
            last_date = last_date - timedelta(days=1)

        found = False
        current_attempt = 0
        while not found and current_attempt < self._config.last_day_gaps_attempts:
            response = self._data_http_client.request(
                self._config.data_daily_URL(symbol, '1m', last_date.year, last_date.month, last_date.day),
                'HEAD',
                retrayable=False,
                raisable=False)
            if response.status_code == 200:
                found = True
            else:   
                current_attempt += 1
                last_date = last_date - timedelta(days=1)

        if not found:
            log().error(f"Last date for symbol {symbol} not found", LogField(key="symbol", value=symbol), LogField(key="last date checked", value=last_date))
            raise ValueError(f"Last date for symbol {symbol} not found")
        
        log().debug("Last date for symbol", LogField(key="symbol", value=symbol), LogField(key="date", value=last_date))

        return last_date

    def get_day_data(self, symbol: str, day: date, resolution: Resolution) -> pd.DataFrame:
        log().info(f"Getting day data", LogField(key="provider", value=self._provider_name), LogField(key="symbol", value=symbol), LogField(key="day", value=day), LogField(key="resolution", value=resolution.name))
        response = self._data_http_client.request(self._config.data_daily_URL(symbol, resolution.name, day.year, day.month, day.day), HTTPMethod.GET)
        data = self._process_CSV_OHLCV(response.content, symbol, resolution.seconds)
        data['provider'] = self._provider_name
        return data

    
    def get_month_data(self, symbol: str, month: date, resolution: Resolution) -> pd.DataFrame:
        log().info(f"Getting month data", LogField(key="provider", value=self._provider_name), LogField(key="symbol", value=symbol), LogField(key="month", value=month), LogField(key="resolution", value=resolution.name))
        response = self._data_http_client.request(self._config.data_monthly_URL(symbol, resolution.name, month.year, month.month), HTTPMethod.GET)
        data = self._process_CSV_OHLCV(response.content, symbol, resolution.seconds)
        data['provider'] = self._provider_name
        return data


@define
class BinanceFactory(ProviderFactory):
    _provider_name: str = field(alias="provider_name")
    _config: BinanceConfig = field(alias="config")

    def create_provider(self) -> BinanceProvider:
        return BinanceProvider(self._provider_name, self._config)


