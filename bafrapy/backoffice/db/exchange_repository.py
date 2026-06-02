from sqlalchemy import select
from sqlalchemy.orm import Session

from bafrapy.backoffice.db.crud import CRUDRepository
from bafrapy.backoffice.models.exchange import Exchange


class ExchangeRepository(CRUDRepository[Exchange, str]):
    def __init__(self, session: Session):
        super().__init__(Exchange, session)

    def get_by_display_name(self, display_name: str) -> Exchange | None:
        return self.session.execute(select(Exchange).where(Exchange.display_name == display_name)).scalar_one_or_none()
