from typing import List

from sqlalchemy.orm import Session

from bafrapy.models import Asset
from bafrapy.repositories.crud import CRUDRepository


class AssetRepository(CRUDRepository[Asset, str]):

    def __init__(self, session: Session):
        super().__init__(Asset, session)

    def get_by_provider(self, provider: str) -> List[Asset]:
        return self.list(Asset.provider_id == provider)

    def get_undated_assets(self, provider: str) -> List[Asset]:
        return self.list(Asset.provider_id == provider, Asset.start_date == None)
