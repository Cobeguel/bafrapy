import datetime
import decimal

from enum import StrEnum
from typing import Optional

from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    ForeignKeyConstraint,
    Numeric,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bafrapy.backoffice.models.base import Base


class Status(StrEnum):
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class Exchange(Base):
    __tablename__ = "exchanges"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="exchanges_pkey"),
        UniqueConstraint("display_name", name="exchanges_display_name_unique"),
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)

    status: Mapped[Status] = mapped_column(
        SAEnum(Status, name="status_enum", create_type=False),
        nullable=False,
        default=Status.ACTIVE,
    )

    last_update: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, default=None)
    type: Mapped[Optional[str]] = mapped_column(String(255), default=None)

    markets: Mapped[list["Market"]] = relationship(
        "Market",
        back_populates="exchange_",
        default_factory=list,
    )


class SyncStatus(StrEnum):
    SYNCED = "SYNCED"
    UNSYNCED = "UNSYNCED"
    STOPPED = "STOPPED"


class MarketStatus(StrEnum):
    LISTED = "LISTED"
    UNLISTED = "UNLISTED"


class Market(Base):
    __tablename__ = "markets"
    __table_args__ = (
        ForeignKeyConstraint(["exchange"], ["exchanges.id"], name="markets_exchange_foreign"),
        PrimaryKeyConstraint("id", name="markets_pkey"),
    )

    id: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
        init=False,
    )

    symbol: Mapped[str] = mapped_column(String(255), nullable=False)
    raw_symbol: Mapped[str] = mapped_column(String(255), nullable=False)
    base: Mapped[str] = mapped_column(String(255), nullable=False)
    quote: Mapped[str] = mapped_column(String(255), nullable=False)
    exchange: Mapped[str] = mapped_column(String(255), nullable=False)

    status: Mapped[Status] = mapped_column(
        SAEnum(Status, name="status_enum", create_type=False),
        nullable=False,
        default=Status.ACTIVE,
    )

    sync_status: Mapped[SyncStatus] = mapped_column(
        SAEnum(SyncStatus, name="sync_status_enum", create_type=False),
        nullable=False,
        default=SyncStatus.UNSYNCED,
    )

    market_status: Mapped[MarketStatus] = mapped_column(
        SAEnum(MarketStatus, name="market_status_enum", create_type=False),
        nullable=False,
        default=MarketStatus.UNLISTED,
    )

    first_date: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, default=None)
    last_date: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, default=None)
    last_update: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, default=None)

    price_min: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(38, 18), default=None)
    price_max: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(38, 18), default=None)
    amount_min: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(38, 18), default=None)
    amount_max: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(38, 18), default=None)
    market_min: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(38, 18), default=None)
    market_max: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(38, 18), default=None)
    cost_min: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(38, 18), default=None)
    cost_max: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(38, 18), default=None)

    exchange_: Mapped[Optional["Exchange"]] = relationship(
        "Exchange",
        back_populates="markets",
        default=None,
    )

    def __post_init__(self):
        self.id = f"{self.exchange}-{self.base}{self.quote}"
