from dataclasses import dataclass

import streamlit as st

from bafrapy.app import base

@dataclass
class DashboardPage(base.BasePage):
    def PageName(cls) -> str:
        return "Dashboard"

    def content(self) -> str:
        st.title("Backtradimus")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(label="Symbols", value=100, delta=0)
        with col2:
            st.metric(label="Strategies", value=100, delta=0)
        with col3:
            st.metric(label="Backtests", value=100, delta=0)
