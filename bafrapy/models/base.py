from datetime import datetime
from typing import Optional

from sqlalchemy import TIMESTAMP, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseModel(DeclarativeBase):
    pass 

class BaseTable(BaseModel):
    __abstract__ = True

    def _normalize_string(self, value: str) -> str:
        return value.strip().replace("/", "").replace(".", "").replace("-", "").replace("_", "").replace(" ", "_").upper()

    status: Mapped[str] = mapped_column(String, default='ACTIVE', nullable=False)
    date_created: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP, default=datetime.now(), nullable=False)
    date_updated: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP, default=datetime.now(), onupdate=datetime.now, nullable=False)


class RelationTable(BaseModel):
    __abstract__ = True

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

