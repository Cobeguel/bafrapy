from abc import ABC, abstractmethod

from streamlit import divider, page_link, sidebar
import streamlit as st


class ClassPropertyDescriptor:
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, instance, owner):
        return self.fget(owner)

def classproperty(func):
    return ClassPropertyDescriptor(func)

class BasePage(ABC):
    @classproperty
    @abstractmethod
    def PageName(cls) -> str:
        pass

    def menu(self):
        with sidebar:
            page_link("pages/dashboard.py", label="Dashboard")
            page_link("pages/data.py", label="Data")
            page_link("pages/download.py", label="Download")

            divider()

    def render(self):
        self.menu()
        self.content()


    @abstractmethod
    def content(self):
        pass
