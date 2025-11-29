from datetime import date, timedelta
from enum import Enum
from typing import Dict, List

import pandas as pd

from attrs import define, field
from dateutil.relativedelta import relativedelta

from bafrapy.datawarehouse.repository import OHLCVRepository
from bafrapy.logger import LogField, LoguruLogger as log
from bafrapy.providers.base import ProviderClient, ResolutionClient
from bafrapy.repositories.mainrepository import MainRepository


@define
class Availability:
    resolution: ResolutionClient
    start_date: date
    end_date: date

class OrderType(Enum):
    DAILY = 'DAYLY'
    MONTHLY = 'MONTHLY'


@define
class DownloadOrder:
    order_type: OrderType
    order_date: date
    resolution: ResolutionClient
    symbol: str
    

@define
class DataTask:
    provider_clients: Dict[str, ProviderClient]
    repository: MainRepository
    data_repository: OHLCVRepository
    min_row_batch: int = field(default=100000)

    def _generate_orders(self, symbol:str, resolution: ResolutionClient, start: date, end: date) -> List[DownloadOrder]:
        if start >= end:
            return []

        orders: list[DownloadOrder] = []
        current = start

        if not (current.year == end.year and current.month == end.month):
            while (current.year, current.month) < (end.year, end.month):
                orders.append(DownloadOrder(order_type=OrderType.MONTHLY, 
                                order_date=current, 
                                resolution=resolution, 
                                symbol=symbol))
                current = current + relativedelta(months=1)

        while current <= end:
            orders.append(DownloadOrder(order_type=OrderType.DAILY, 
                                order_date=current, 
                                resolution=resolution, 
                                symbol=symbol))
            current = current + timedelta(days=1)

        return orders

    def _download(self, provider_client: ProviderClient, order: DownloadOrder) -> pd.DataFrame:
        if order.order_type == OrderType.MONTHLY:
            data = provider_client.get_month_data(order.symbol, order.order_date, order.resolution)
        else:
            data = provider_client.get_day_data(order.symbol, order.order_date, order.resolution)
        return data


    def run(self, providerName: str, symbol: str):
        try:
            provider_client = self.provider_clients[providerName]
        except KeyError:
            raise ValueError(f"Provider {providerName} not found")

        orders_by_resolutions: List[List[DownloadOrder]] = []
        with self.repository.start_session() as repo:
            provider = repo.providers.get_by_external_name(providerName)
            asset = repo.assets.get_by_provider_and_symbol(provider.id, symbol)

            if asset is None:
                raise ValueError(f"Asset {symbol} not found for provider {providerName}")

            if asset.first_date is None:
                raise ValueError(f"Asset {asset.id} has no first date set")
            
            available_resolutions = provider.resolutions

            for resolution in available_resolutions:
                start_date = asset.first_date.date()
                
                availability = self.data_repository.symbol_availability(provider.external_name, asset.symbol, resolution.seconds)
                if availability is not None and availability.last_date is not None:
                    start_date = max(start_date, availability.last_date)
                    if start_date is not None:
                        start_date = start_date + timedelta(days=1)
                # TODO: Fetch real last date from provider or handle unupdated assets
                end_date = date.today() - timedelta(days=2)
                resolution = ResolutionClient(name=resolution.provider_display, seconds=resolution.seconds)
                orders_by_resolutions.append(self._generate_orders(asset.symbol, resolution, start_date, end_date))
            

        for orders in orders_by_resolutions:
            data: pd.DataFrame = None
            for order in orders:
                try:
                    chunk = self._download(provider_client, order)
                except Exception as e:
                    log().error("Error downloading data", LogField(key="order", value=order), LogField(key="error", value=e))
                    raise e
                if chunk is None or chunk.empty:
                    continue
                if data is None or data.empty:
                    data = chunk
                else:
                    data = pd.concat([data, chunk], ignore_index=True)

                if len(data) >= self.min_row_batch:
                    log().info("Inserting data", LogField(key="count", value=len(data)))
                    self.data_repository.insert_data(data)
                    data = None

            if data is not None and not data.empty:
                log().info("Inserting data", LogField(key="count", value=len(data)))
                self.data_repository.insert_data(data)

            self.data_repository.clean_or_optimize_symbol(providerName, symbol)




