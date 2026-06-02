from typing import List, Optional, Type

from sqlalchemy import func, inspect, select
from sqlalchemy.orm import Session

from bafrapy.backoffice.db.base import ID, AbstractCRUDRepository, T


class CRUDRepository(AbstractCRUDRepository[T, ID]):
    def __init__(self, model: Type[T], session: Session):
        self.model = model
        self.session = session

    def save(self, instance: T) -> T:
        if inspect(instance).transient:
            self.session.add(instance)
        else:
            instance = self.session.merge(instance)
        return instance

    def save_all(self, instances: List[T]) -> List[T]:
        return [self.save(instance) for instance in instances]

    def get(self, id: ID) -> Optional[T]:
        return self.session.get(self.model, id)

    def list(self) -> List[T]:
        return list(self.session.execute(select(self.model)).scalars().all())

    def remove(self, id: ID) -> bool:
        instance = self.get(id)
        if instance is None:
            return False
        self.session.delete(instance)
        return True

    def count(self) -> int:
        return self.session.execute(select(func.count()).select_from(self.model)).scalar()
