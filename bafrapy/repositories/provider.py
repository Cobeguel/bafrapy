from sqlalchemy.orm import Session

from bafrapy.models import Provider
from bafrapy.repositories.crud import CRUDRepository


class ProviderRepository(CRUDRepository[Provider, str]):
    
    def __init__(self, session: Session):
        super().__init__(Provider, session)