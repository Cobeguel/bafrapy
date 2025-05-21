
from typing import Optional

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from bafrapy.models.base import BaseTable


class Resolution(BaseTable):
    __tablename__ = "def_resolution"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    display_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    seconds: Mapped[int] = mapped_column(Integer, nullable=False)
