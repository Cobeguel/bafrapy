
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bafrapy.models.base import BaseTable

from bafrapy.models.providers_resolutions import ProviderResolution

if TYPE_CHECKING:
    from bafrapy.models.assets import Asset
    from bafrapy.models.resolution import Resolution

class Provider(BaseTable):
    __tablename__ = "def_providers"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    display_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    assets: Mapped[List["Asset"]] = relationship("Asset", back_populates="provider")
    resolutions: Mapped[List["Resolution"]] = relationship(
        secondary="def_providers_def_resolution", overlaps="resolution")
