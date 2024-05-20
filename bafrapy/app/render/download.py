from dataclasses import dataclass, field
from datetime import date
from enum import Enum

import pandas as pd
import streamlit as st

from bafrapy.app import base
from bafrapy.app.cache import binance_symbols, data_repository, management_repository
from bafrapy.data_provider.base_provider import BaseProvider
from bafrapy.data_provider.binance_provider import BinanceProvider


class OHLCVState(Enum):
    ORIGINAL = 'ORIGINAL'
    GAP = 'GAP'

@dataclass
class DownloadPage(base.BasePage):
    provider_name: str = field(default=[getattr(provider, 'name') for provider in (management_repository().providers.list())][0], init=False)
    symbol: str = field(default="", init=False)
    provider: BaseProvider = field(default=None, init=False)
    
    def __post_init__(self):
        self.provider = BinanceProvider()

    def PageName(cls) -> str:
        return "Download"
    
    def update_provider(self):
        if self.provider_name == "Binance":
            self.provider = BinanceProvider()

    def content(self) -> str:
        provider_names = [getattr(provider, 'name') for provider in (management_repository().providers.list())]
        self.provider_name = st.selectbox("Provider", options=provider_names, key='provider_select', on_change=self.update_provider)
        
        # TODO: Parametrice the symbols extraction by provider
        self.symbol = st.selectbox("Available symbols", binance_symbols())

        if self.symbol != "":
            availability = self.provider.availability(self.symbol)
            start = availability[0].year
            end = availability[1].year
    
            years = list(range(start, end + 1))
            state = st.data_editor(
                pd.DataFrame({
                    'Year': years,
                    'Symbol': [self.symbol] * len(years),
                    'Status': ["NOT DOWNLOADED"] * len(years),
                    'Select': [False] * len(years)
                    },
                ),
                hide_index=True,
                disabled=['Year', 'Symbol', 'Status']
            )
            st.button("Download selected data", on_click=self.download_data, args=(state.loc[state['Select'] == True, 'Year'].tolist(),))

    def download_data(self, years):
        if len(years) < 2 or years[0] > years[-1]:
            st.toast(f"Invalid range of years {years}", icon=":material/error:")
        else:
            st.toast(f"Downloading data from years {years}", icon=":material/download:")

            for data in self.provider.get_data(self.symbol, date(year=years[0], month=1, day=1), date(year=years[-1], month=12, day=31)):
                resample_time = str(data['resolution'].iloc[0]) + 's'
                data['state'] = OHLCVState.ORIGINAL.value
                data.set_index('time', inplace=True)
                data = data.resample(resample_time).asfreq()
                data.reset_index(inplace=True)
                data['close'] = data['close'].fillna(method='ffill')
                data['open'] = data['open'].fillna(data['close'])
                data['high'] = data['high'].fillna(data['close'])
                data['low'] = data['low'].fillna(data['close'])
                data['volume'] = data['volume'].fillna(0)
                data['provider'] = self.provider_name
                data['state'] = data['state'].fillna(OHLCVState.GAP.value)
                data['symbol'] = self.symbol

                try:
                    data_repository().insert_data(data)
                except Exception as e:
                    st.toast(f"Error inserting data: {e}", icon=":material/error:")
                    
            data_repository().optimize_table()