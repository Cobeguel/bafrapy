from abc import ABC, abstractmethod

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

    @abstractmethod
    def render(self) -> str:
        pass
