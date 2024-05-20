from streamlit import set_page_config

from bafrapy.app.handler import route
from bafrapy.app.render.dashboard import DashboardPage

set_page_config(layout="wide")

route(DashboardPage)
