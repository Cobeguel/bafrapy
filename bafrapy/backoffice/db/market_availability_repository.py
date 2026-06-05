from sqlalchemy.orm import Session

from bafrapy.backoffice.db.crud import CRUDRepository
from bafrapy.backoffice.models import MarketAvailability


class MarketAvailabilityRepository(CRUDRepository[MarketAvailability, str]):
    def __init__(self, session: Session):
        super().__init__(MarketAvailability, session)
