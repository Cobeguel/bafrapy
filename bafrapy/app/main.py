import streamlit as st
from bafrapy.app.handler import route

from bafrapy.app.render.dashboard import DashboardPage

route(DashboardPage)
