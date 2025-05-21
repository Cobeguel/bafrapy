from typing import List, Optional, Type

from sqlalchemy import inspect
from sqlalchemy.orm import Session

from bafrapy.repositories.base import ID, AbstractRepository, T


class CRUDRepository(AbstractRepository[T, ID]):

    def __init__(self, model: Type[T], session: Session):
        self.model = model
        self.session = session

    def save(self, instance: T) -> T:
        if inspect(instance).transient:
            self.session.add(instance)
        else:
            self.session.merge(instance)
        return instance

    def save_all(self, instances: List[T]):
        for instance in instances:
            if inspect(instance).transient:
                self.session.add(instance)
            else:
                self.session.merge(instance)
        return instances

    def get_by_id(self, id: ID) -> Optional[T]:
        return self.session.query(self.model).filter(self.model.id == id).first()

    def archive(self, instance: T) -> bool:
        instance.status = 'ARCHIVED'
        self.session.add(instance)
        return True

    def exists_by_id(self, id: ID) -> bool:
        return self.get_by_id(id) is not None

    def count(self, *filters: any) -> int:
        return self.session.query(self.model).filter(*filters).count()
    
    def list(self, *filters: any) -> List[T]:
        return self.session.query(self.model).filter(*filters).all()