import uuid

from sqlalchemy.orm import Session

from bafrapy.backoffice.db.crud import CRUDRepository
from bafrapy.backoffice.models.tasks import Task


class TaskRepository(CRUDRepository[Task, uuid.UUID]):
    def __init__(self, session: Session):
        super().__init__(Task, session)
