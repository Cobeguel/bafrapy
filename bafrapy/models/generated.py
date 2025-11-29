import datetime

from typing import Optional

from sqlalchemy import (
    CHAR,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Provider(Base):
    __tablename__ = 'providers'

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(255), nullable=False, server_default=text("'ACTIVE'"))
    display_name: Mapped[Optional[str]] = mapped_column(String(255))
    external_name: Mapped[Optional[str]] = mapped_column(String(255))

    resolutions: Mapped[list['Resolution']] = relationship('Resolution', secondary='providers_resolutions', back_populates='providers')
    assets: Mapped[list['Asset']] = relationship('Asset', back_populates='provider')


class Resolution(Base):
    __tablename__ = 'resolutions'
    __table_args__ = (
        Index('resolutions_seconds_unique', 'seconds', unique=True),
    )

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(255), nullable=False, server_default=text("'ACTIVE'"))
    display_name: Mapped[Optional[str]] = mapped_column(String(255))
    seconds: Mapped[Optional[int]] = mapped_column(Integer)
    provider_display: Mapped[Optional[str]] = mapped_column(String(255))

    providers: Mapped[list['Provider']] = relationship('Provider', secondary='providers_resolutions', back_populates='resolutions')


class Asset(Base):
    __tablename__ = 'assets'
    __table_args__ = (
        ForeignKeyConstraint(['provider'], ['providers.id'], ondelete='SET NULL', name='assets_provider_foreign'),
        Index('assets_provider_foreign', 'provider')
    )

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(255), nullable=False, server_default=text("'ACTIVE'"))
    symbol: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[Optional[str]] = mapped_column(String(255))
    base: Mapped[Optional[str]] = mapped_column(String(255))
    quote: Mapped[Optional[str]] = mapped_column(String(255))
    first_date: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    last_date: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    provider_id: Mapped[Optional[str]] = mapped_column('provider', CHAR(36))

    provider: Mapped[Optional['Provider']] = relationship('Provider', back_populates='assets')


t_providers_resolutions = Table(
    'providers_resolutions', Base.metadata,
    Column('id', INTEGER, primary_key=True),
    Column('providers_id', CHAR(36)),
    Column('resolutions_id', CHAR(36)),
    ForeignKeyConstraint(['providers_id'], ['providers.id'], ondelete='SET NULL', name='providers_resolutions_providers_id_foreign'),
    ForeignKeyConstraint(['resolutions_id'], ['resolutions.id'], ondelete='SET NULL', name='providers_resolutions_resolutions_id_foreign'),
    Index('providers_resolutions_providers_id_foreign', 'providers_id'),
    Index('providers_resolutions_resolutions_id_foreign', 'resolutions_id')
)
