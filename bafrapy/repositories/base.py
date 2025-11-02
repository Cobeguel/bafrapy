from abc import ABC, abstractmethod
from typing import Generic, List, Optional, TypeVar
from sqlmodel import SQLModel


T = TypeVar('T', bound=SQLModel)
ID = TypeVar('ID')

class AbstractRepository(ABC, Generic[T, ID]):

    @abstractmethod
    def save(self, instance: T) -> T:
        pass

    @abstractmethod
    def save_all(self, instances: List[T]):
        pass

    @abstractmethod
    def get_by_id(self, id: ID) -> Optional[T]:
        pass

    @abstractmethod
    def list(self, *filters: any) -> List[T]:
        pass

    @abstractmethod
    def archive(self, instance: T) -> bool:
        pass

    @abstractmethod
    def exists_by_id(self, id: ID) -> bool:
        pass

    @abstractmethod
    def count(self, *filters: any) -> int:
        pass