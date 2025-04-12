import streamlit as st

from bafrapy.app.base import BasePage


def route(page : BasePage) -> None:
    if page.PageName not in st.session_state:
        st.session_state[page.PageName] = page()
    st.session_state[page.PageName].render()
