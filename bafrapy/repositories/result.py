from typing import List

from sqlalchemy.orm import Session

from bafrapy.models import Result
from bafrapy.repositories.crud import CRUDRepository


class ResultRepository(CRUDRepository[Result, str]):

    def __init__(self, session: Session):
        super().__init__(Result, session)

