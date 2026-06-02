from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from bafrapy.backoffice.db.crud import CRUDRepository
from bafrapy.backoffice.models import Market


class MarketRepository(CRUDRepository[Market, str]):
    def __init__(self, session: Session):
        super().__init__(Market, session)

    def get_by_exchange_and_symbol(self, exchange_id: str, symbol: str) -> Market | None:
        return self.session.execute(
            select(Market).where(Market.exchange == exchange_id, Market.symbol == symbol)
        ).scalar_one_or_none()

    def list_by_exchange(self, exchange_id: str) -> List[Market]:
        return list(self.session.execute(select(Market).where(Market.exchange == exchange_id)).scalars().all())
