from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bafrapy.models.base import RelationTable

if TYPE_CHECKING:
    from bafrapy.models.resolution import Resolution


class ProviderResolution(RelationTable):
    __tablename__ = "def_providers_def_resolution"

    provider_id: Mapped[Optional[str]] = mapped_column("def_providers_id", String, ForeignKey("def_providers.id", ondelete="SET NULL"), nullable=True)
    resolution_id: Mapped[Optional[str]] = mapped_column("def_resolution_id", String, ForeignKey("def_resolution.id", ondelete="SET NULL"), nullable=True)
    resolution: Mapped["Resolution"] = relationship("Resolution",  primaryjoin="foreign(ProviderResolution.resolution_id) == Resolution.id")