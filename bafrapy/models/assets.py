from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import TIMESTAMP, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bafrapy.models.base import BaseTable
from bafrapy.models.providers import Provider

if TYPE_CHECKING:
    from bafrapy.models.providers import Provider

class Asset(BaseTable):
    __tablename__ = "def_assets"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    symbol: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    base: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    quote: Mapped[str] = mapped_column(String(255), nullable=False)
    start_date: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP, nullable=True)
    provider_id: Mapped[Optional[str]] = mapped_column("provider", ForeignKey("def_providers.id", ondelete="SET NULL"), nullable=True)

    provider: Mapped[Optional[Provider]] = relationship(
        "Provider",
        foreign_keys= lambda: Asset.provider_id,
        back_populates= "assets"
    )

    def __init__(self, provider: Provider, symbol: str, base: str, quote: str):
        self.id = f"{provider.id}_{self._normalize_string(symbol)}"
        self.symbol = self._normalize_string(symbol)
        self.base = self._normalize_string(base)
        self.quote = self._normalize_string(quote)
        self.provider = provider