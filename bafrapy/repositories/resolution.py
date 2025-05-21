from sqlalchemy.orm import Session

from bafrapy.models import Resolution
from bafrapy.repositories.crud import CRUDRepository


class ResolutionRepository(CRUDRepository[Resolution, str]):
    
    def __init__(self, session: Session):
        super().__init__(Resolution, session)