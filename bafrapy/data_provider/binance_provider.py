import io
import zipfile
import zlib

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Generator, List, Tuple

import pandas as pd
import requests

from bafrapy.data_provider.base_provider import BaseProvider


@dataclass
class BinanceProvider(BaseProvider):

    @property
    def api_root_endpoint(self):
        return "https://api.binance.com/api/v3"

    @property
    def api_info_endpoint(self):
        return self.api_root_endpoint + "/exchangeInfo"

    @property
    def api_aggTrades(self):
        return self.api_root_endpoint + "/aggTrades"

    @property
    def data_base_URL(self):
        return "https://data.binance.vision/data"

    @property
    def data_daily_klines_URL(self):
        return  self.data_base_URL + "/spot/daily/klines"

    @property
    def data_monthly_klines_URL(self):
        return  self.data_base_URL + "/spot/monthly/klines"

    def _build_data_url(self, symbol: str, resolution: str, date: datetime, monthly: bool) -> str:
        # Current example url https://data.binance.vision/?prefix=data/spot/daily/klines/1INCHBTC/1h/1INCHBTC-1h-2023-05-26.zip
        if monthly:
            return  f'{self.data_monthly_klines_URL}/{symbol}/{resolution}/{symbol}-{resolution}-{date.strftime("%Y-%m")}.zip'
        else:
            return f'{self.data_daily_klines_URL}/{symbol}/{resolution}/{symbol}-{resolution}-{date.strftime("%Y-%m-%d")}.zip'

    def list_available_symbols(self) -> list[str]:
        response = requests.get(self.api_info_endpoint)
        response.raise_for_status()
        body = response.json()
        symbols = []
        for item in body['symbols']:
            symbols.append(item['symbol'])
        return symbols
    
    def get_data_resolutions(self, symbol: str = "") -> List[str]:
        pass

    def get_first_available_date(self, symbol: str) -> date:
        response = requests.get(self.api_aggTrades, params={"symbol": symbol, "fromId": 1})
        response.raise_for_status()

        day = datetime.fromtimestamp(response.json()[0]['T'] / 1000)

        return date(day.year, day.month, day.day)

    def _data_range_months(self, start_date: date, end_date: date) -> List[date]:
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()
        months = []

        if start_date == end_date:
            return [start_date.replace(day=1)]

        month = start_date.replace(day=1)
        current_date = date.today()

        while month <= end_date:
            if month.month == current_date.month and month.year == current_date.year and end_date.month == current_date.month:
                break

            months.append(month)
            month = (month + timedelta(days=32)).replace(day=1)

        return months

    def availability(self, symbol: str) -> Tuple[datetime, datetime]:
        first_date = self.get_first_available_date(symbol)
        last_date = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return (first_date, last_date)

    def get_data(self, symbol: str, start_date: date, end_date: date) -> Generator[pd.DataFrame, None, None]:
        if start_date.year == end_date.year:
            end_date.replace(month=12)

        availability = self.availability(symbol)
        if start_date.year == availability[0].year:
            start_date = availability[0]

        if end_date.year == availability[1].year:
            end_date = availability[1]

        data_range = self._data_range_months(start_date, end_date)
        reqs = []
        for date in data_range:
            # TODO evaluate resolutions
            reqs.append(requests.Request('GET', self._build_data_url(symbol, '1s', date, monthly=True)))

        for req in reqs:
            yield from self._request_data(req)

        return None

    def _handle_response(self, response: requests.Response) -> Generator[pd.DataFrame, None, None]:
        try:
            zip_bytes = io.BytesIO(response.content)
            with zipfile.ZipFile(zip_bytes) as zf:
                csv_file_name = zf.namelist()[0]
                with zf.open(csv_file_name) as csv_file:
                    with io.TextIOWrapper(csv_file, encoding='utf-8') as text_file:
                        chunk_size = 10**6 
                        lines = []
                        for line in text_file:
                            lines.append(line)
                            if len(lines) >= chunk_size:
                                df_chunk = pd.read_csv(io.StringIO(''.join(lines)), header=None, dtype=str)
                                df_chunk = df_chunk.iloc[:, :6]
                                df_chunk.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
                                df_chunk[['open', 'high', 'low', 'close', 'volume']] = df_chunk[['open', 'high', 'low', 'close', 'volume']].applymap(lambda x: Decimal(x))
                                df_chunk['time'] = pd.to_datetime(df_chunk['time'].astype(int), unit='ms')
                                df_chunk['resolution'] = 1

                                yield df_chunk
                                lines = []

                        if lines:
                            df_chunk = pd.read_csv(io.StringIO(''.join(lines)), header=None, dtype=str)
                            
                            if len(df_chunk.columns) >= 6:
                                df_chunk = df_chunk.iloc[:, :6]
                                df_chunk.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
                                df_chunk[['open', 'high', 'low', 'close', 'volume']] = df_chunk[['open', 'high', 'low', 'close', 'volume']].applymap(lambda x: Decimal(x))
                                df_chunk['time'] = pd.to_datetime(df_chunk['time'].astype(int), unit='ms')
                                df_chunk['resolution'] = 1

                                yield df_chunk

        except zipfile.BadZipFile:
            raise ValueError("File is not a valid ZIP file or is corrupted")

    def get_data_resolutions(self, symbol: str = "") -> List[str]:
        pass
