from typing import List

from streamlit import cache_resource
import streamlit as st

from bafrapy.data_provider.binance_provider import BinanceProvider
from bafrapy.models.data.repo import DataRepository
from bafrapy.models.management.repo import ManagementRepository


@cache_resource
def data_repository() -> DataRepository:
    return DataRepository(pool=True)

@cache_resource
def management_repository() -> ManagementRepository:
		return ManagementRepository()

@st.cache_data
def binance_symbols() -> List[str]:
    return sorted(BinanceProvider().list_available_symbols())
