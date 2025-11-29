from typing import List

from sqlalchemy.orm import Session

from bafrapy.models import Asset
from bafrapy.repositories.crud import CRUDRepository


class AssetRepository(CRUDRepository[Asset, str]):
    def __init__(self, session: Session):
        super().__init__(Asset, session)

    def get_by_provider_and_symbol(self, provider: str, symbol: str) -> Asset:
        return self.get(Asset.provider_id == provider, Asset.symbol == symbol)

    def get_by_provider(self, provider: str) -> List[Asset]:
        return self.list(Asset.provider_id == provider)

    def get_undated_assets(self, provider_id: str, limit: int = 100) -> List[Asset]:
        return self.list(
            Asset.provider_id == provider_id,
            Asset.first_date.is_(None),
            order_by=Asset.symbol,
            limit=limit,
        )

    def count_undated_assets(self, provider_id: str) -> int:
        return self.count(Asset.provider_id == provider_id, Asset.first_date.is_(None))
