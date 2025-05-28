import io
import json
import zipfile

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Iterator, List

import pandas as pd
import requests

from attrs import define, field
from dateutil.relativedelta import relativedelta
from yarl import URL

from bafrapy.libs.datetime import normalize_mixed_timestamp
from bafrapy.logger.log import LogField, LoguruLogger as log
from bafrapy.providers.base import (
    BackoffRequest,
    Provider,
    ProviderFactory,
    Resolution,
    Symbol,
)


@define
class BinanceConfig:
    _api_base_domain: str = field(alias="api_base_domain")
    _exchange_info_endpoint: str = field(alias="exchange_info_endpoint")
    _aggTrades_endpoint: str = field(alias="aggTrades_endpoint")

    _data_vision_domain: str = field(alias="data_vision_domain")
    _data_daily_klines_URL: str = field(alias="data_daily_klines_URL")
    _data_monthly_klines_URL: str = field(alias="data_monthly_klines_URL")

    _max_last_date_attempts: int = field(alias="max_last_date_attempts")

    @classmethod
    def from_config_file(cls, config_file: str):
        with open(config_file, 'r') as f:
            config = json.load(f)
        try:
            config = config['binance']
            return cls(
                api_base_domain=config['api_domain'],
                exchange_info_endpoint=config['exchange_info_endpoint'],
                aggTrades_endpoint=config['aggTrades_endpoint'],
                data_vision_domain=config['data_vision_domain'],
                data_daily_klines_URL=config['data_daily_klines_URL'],
                data_monthly_klines_URL=config['data_monthly_klines_URL']
            )
        except KeyError:
            raise ValueError(f"Invalid configuration file {config_file} for binance provider")

    def symbols_URL(self):
        return URL(self._api_base_domain) / self._exchange_info_endpoint
    
    def aggTrades_URL(self):
        return URL(self._api_base_domain) / self._aggTrades_endpoint 
    
    # Example https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-04-29.zip
    # Example https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-2025-04-29.zip
    def data_vision_URL(self, symbol: str, resolution: str, date: datetime, monthly: bool = False):
        if monthly:
            return URL(self._data_vision_domain) / self._data_monthly_klines_URL / symbol / resolution / f"{symbol}-{resolution}-{date.strftime('%Y-%m')}.zip"
        else:
            return URL(self._data_vision_domain) / self._data_daily_klines_URL / symbol / resolution / f"{symbol}-{resolution}-{date.strftime('%Y-%m-%d')}.zip"

@define
class BinanceProvider(Provider):
    _provider_name: str = field(alias="provider_name")
    _config: BinanceConfig = field(alias="config")
    _backoff_request: BackoffRequest = field(alias="backoff_request")
    _api_session: requests.Session = field(factory=requests.Session, init=False, repr=False)
    _data_session: requests.Session = field(factory=requests.Session, init=False, repr=False)

    def list_available_symbols(self) -> List[Symbol]:
        response = self._api_session.get(self._config.symbols_URL())
        response.raise_for_status()
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
        params = {'symbol': symbol, 'fromId': 0, 'limit': 1}
        response = self._api_session.get(self._config.aggTrades_URL(), params=params)
        response.raise_for_status()

        body = response.json()
        first_date = datetime.fromtimestamp(body[0]['T'] / 1000)

        log().debug("First date for symbol", LogField(key="symbol", value=symbol), LogField(key="date", value=first_date))
        return first_date
    
    def symbol_last_date(self, symbol: str) -> date:
        params = {'symbol': symbol, 'limit': 1}
        response = self._api_session.get(self._config.aggTrades_URL(), params=params)
        response.raise_for_status()

        body = response.json()
        last_date = datetime.fromtimestamp(body[0]['T'] / 1000)
        if last_date.date() == date.today():
            last_date = last_date - timedelta(days=1)

        found = False
        current_attempt = 0
        while not found and current_attempt < self._config._max_last_date_attempts:
            response = requests.head(self._config.data_vision_URL(symbol, '1m', last_date))
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
        response = self._data_session.get(self._config.data_vision_URL(symbol, resolution.name, day, False))
        return self._process_OHLCV(response.content, symbol, resolution.seconds)
    
    def get_month_data(self, symbol: str, month: date, resolution: Resolution) -> pd.DataFrame:
        response = self._data_session.get(self._config.data_vision_URL(symbol, resolution.name, month, True))
        return self._process_OHLCV(response.content, symbol, resolution.seconds)
    
    def get_data(self, symbol: str, start_date: date, resolution: Resolution) -> Iterator[pd.DataFrame]:
        last_date = self.symbol_last_date(symbol)
        if start_date > last_date:
            return None

        between_months = (last_date.year - start_date.year) * 12 + (last_date.month - start_date.month)
        between_days = (last_date.day - start_date.day)

        
        if between_months > 0 and not between_days == 1:
            current_date = start_date
            for _ in range(between_months):
                response = self._data_session.get(self._config.data_vision_URL(symbol, resolution.name, current_date, True))
                yield self._process_OHLCV(
                        response.content, 
                        symbol,
                        resolution.seconds)
                current_date = current_date + relativedelta(months=1)
        if between_days > 0:
            if between_months > 0:
                current_date = date(last_date.year, last_date.month, 1)
            else:
                current_date = start_date
            for _ in range(between_days):
                response = self._data_session.get(self.config.data_vision_URL(symbol, resolution.name, current_date, False))

                yield self._process_OHLCV(
                        response.content, 
                        symbol,
                        resolution.seconds)
            current_date = current_date + relativedelta(days=1)
        
        return None


    def _process_OHLCV(self, bytes: bytes, symbol: str, resolution: int) -> pd.DataFrame:
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

@define
class BinanceFactory(ProviderFactory):
    _provider_name: str = field(alias="provider_name")
    _config: BinanceConfig = field(alias="config")
    _backoff_request: BackoffRequest = field(alias="backoff_request")

    @classmethod
    def from_config_file(cls, config_file: str):
        with open(config_file, 'r') as f:
            config = json.load(f)
            try:
                return cls(
                    provider_name=config['provider_name'],
                    config=BinanceConfig(
                        api_base_domain=config['routes']['api_domain'],
                        exchange_info_endpoint=config['routes']['exchange_info_endpoint'],
                        aggTrades_endpoint=config['routes']['aggTrades_endpoint'],
                        data_vision_domain=config['routes']['data_vision_domain'],
                        data_daily_klines_URL=config['routes']['data_daily_klines_URL'],
                        data_monthly_klines_URL=config['routes']['data_monthly_klines_URL'],
                        max_last_date_attempts=config['routes']['max_last_date_attempts']
                    ),
                    backoff_request=BackoffRequest(
                        timeout=config['backoff']['timeout'],
                        max_tries=config['backoff']['max_tries'],
                        giveup_codes=config['backoff']['giveup_codes']
                    )
                )
            except KeyError:
                raise ValueError(f"Invalid configuration file {config_file} for binance provider")

    def create_provider(self) -> BinanceProvider:
        return BinanceProvider(self._provider_name, self._config, self._backoff_request)

