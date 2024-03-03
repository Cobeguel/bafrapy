from dataclasses import dataclass
from abc import ABC, abstractmethod
from nicegui import ui
from typing import Optional
from pathlib import Path

def add_head_html() -> None:
    ui.add_head_html(f"<style>{(Path(__file__).parent / 'static' / 'style.css').read_text()}</style>")

def add_header(menu: Optional[ui.left_drawer] = None) -> None:
    menu_items = {
        'Dashboard': '/',
    }

    with ui.header() \
            .classes('items-center duration-200 py-2 px-4 no-wrap') \
            .style('box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1)'):
        if menu:
            ui.button(on_click=menu.toggle, icon='menu').props('flat color=white round').classes('lg:hidden')

        with ui.row().classes('max-[1050px]:hidden'):
            for title_, target in menu_items.items():
                ui.link(title_, target).classes(replace='text-lg text-white')

        with ui.row().classes('min-[1051px]:hidden'):
            with ui.button(icon='more_vert').props('flat color=white round'):
                with ui.menu().classes('bg-primary text-white text-lg'):
                    for title_, target in menu_items.items():
                        ui.menu_item(title_, on_click=lambda target=target: ui.open(target))

class PageBase(ABC):
    def render(self) -> None:
        add_head_html()
        add_header()
        self.body()

    @abstractmethod
    def body(self) -> None:
        pass
