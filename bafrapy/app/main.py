from nicegui import app, ui
from bafrapy.app.pages.dashboard import DashboardPage
import logging


@ui.page("/")
def _dashboard() -> None:
    DashboardPage().render()

logging.basicConfig(level=logging.DEBUG)
ui.run(uvicorn_reload_includes='*.py, *.css, *.html', reconnect_timeout=10.0)
