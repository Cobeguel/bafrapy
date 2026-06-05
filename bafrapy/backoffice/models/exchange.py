import datetime
import decimal

from dataclasses import field
from enum import StrEnum
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    Enum as SAEnum,
    ForeignKeyConstraint,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bafrapy.backoffice.models.base import Base

ExchangeResolution = Table(
    "exchanges_resolutions",
    Base.metadata,
    Column("id", Integer),
    Column("exchanges_id", String(255), nullable=False),
    Column("resolutions_id", String(255), nullable=False),
    ForeignKeyConstraint(
        ["exchanges_id"],
        ["exchanges.id"],
        name="exchanges_resolutions_exchanges_id_foreign",
    ),
    ForeignKeyConstraint(
        ["resolutions_id"],
        ["resolutions.id"],
        name="exchanges_resolutions_resolutions_id_foreign",
    ),
    PrimaryKeyConstraint("id", name="exchanges_resolutions_pkey"),
)


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
    resolutions: Mapped[list["Resolution"]] = relationship(
        "Resolution",
        secondary=ExchangeResolution,
        back_populates="exchanges",
        default_factory=list,
    )


class SyncStatus(StrEnum):
    SYNCED = "SYNCED"
    UNSYNCED = "UNSYNCED"
    STOPPED = "STOPPED"
    SYNCING = "SYNCING"


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
        init=False,
    )
    market_availabilities: Mapped[list["MarketAvailability"]] = relationship(
        "MarketAvailability",
        back_populates="market_",
        default_factory=list,
    )

    # id format: {exchange}-{base}{quote}
    def __post_init__(self):
        if not self.exchange:
            raise ValueError("Market exchange cannot be empty")
        if not self.base:
            raise ValueError("Market base cannot be empty")
        if not self.quote:
            raise ValueError("Market quote cannot be empty")
        self.id = f"{self.exchange}-{self.base}{self.quote}"


class Resolution(Base):
    __tablename__ = "resolutions"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="resolutions_pkey"),
        UniqueConstraint("code", name="resolutions_code_unique"),
        UniqueConstraint("seconds", name="resolutions_seconds_unique"),
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    code: Mapped[str] = mapped_column(String(255), nullable=False)
    last_update: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, default=None)

    exchanges: Mapped[list["Exchange"]] = relationship(
        "Exchange",
        secondary=ExchangeResolution,
        back_populates="resolutions",
        default_factory=list,
    )
    market_availabilities: Mapped[list["MarketAvailability"]] = relationship(
        "MarketAvailability",
        back_populates="resolution_",
        default_factory=list,
    )


class MarketAvailability(Base):
    __tablename__ = "market_availability"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market"], ["markets.id"], ondelete="SET NULL", name="market_availability_market_foreign"
        ),
        ForeignKeyConstraint(
            ["resolution"], ["resolutions.id"], ondelete="SET NULL", name="market_availability_resolution_foreign"
        ),
        PrimaryKeyConstraint("id", name="market_availability_pkey"),
    )

    id: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
        init=False,
    )
    first_date: Mapped[Optional[datetime.datetime]] = mapped_column("fisrt_date", DateTime, default=None)
    last_date: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, default=None)
    resolution: Mapped[Optional[str]] = mapped_column(String(255), default=None)
    market: Mapped[Optional[str]] = mapped_column(String(255), default=None)
    resolution_seconds: int = field(kw_only=True)

    market_: Mapped[Optional["Market"]] = relationship(
        "Market",
        back_populates="market_availabilities",
        init=False,
    )
    resolution_: Mapped[Optional["Resolution"]] = relationship(
        "Resolution",
        back_populates="market_availabilities",
        init=False,
    )

    @classmethod
    def create(cls, *, market: str, resolution: "Resolution", **kwargs) -> "MarketAvailability":
        return cls(
            market=market,
            resolution=resolution.id,
            resolution_seconds=resolution.seconds,
            **kwargs,
        )

    def __post_init__(self):
        if not self.market:
            raise ValueError("MarketAvailability market cannot be empty")
        if not self.resolution:
            raise ValueError("MarketAvailability resolution cannot be empty")
        if self.resolution_seconds <= 0:
            raise ValueError("MarketAvailability resolution_seconds must be positive")
        self.id = f"{self.market}-{self.resolution_seconds}"
