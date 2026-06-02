from abc import ABC, abstractmethod
from typing import Generic, List, Optional, TypeVar

from sqlalchemy.orm import DeclarativeBase

T = TypeVar("T", bound=DeclarativeBase)
ID = TypeVar("ID")


class AbstractCRUDRepository(ABC, Generic[T, ID]):
    @abstractmethod
    def save(self, instance: T) -> T:
        pass

    @abstractmethod
    def save_all(self, instances: List[T]):
        pass

    @abstractmethod
    def get(self, id: ID) -> Optional[T]:
        pass

    @abstractmethod
    def list(self) -> List[T]:
        pass

    @abstractmethod
    def remove(self, id: ID) -> bool:
        pass

    @abstractmethod
    def count(self) -> int:
        pass
