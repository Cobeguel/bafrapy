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
    text,
)
from sqlalchemy.dialects.mysql import INTEGER
from sqlmodel import Field, Relationship, SQLModel


class Provider(SQLModel, table=True):
    __tablename__ = 'providers'

    id: str = Field(sa_column=Column('id', CHAR(36), primary_key=True))
    status: str = Field(sa_column=Column('status', String(255), nullable=False, server_default=text("'ACTIVE'")))
    display_name: Optional[str] = Field(default=None, sa_column=Column('display_name', String(255)))
    external_name: Optional[str] = Field(default=None, sa_column=Column('external_name', String(255)))

    assets: list['Asset'] = Relationship(back_populates='provider')
    providers_resolutions: list['ProvidersResolution'] = Relationship(back_populates='provider')


class Resolution(SQLModel, table=True):
    __tablename__ = 'resolutions'
    __table_args__ = (
        Index('resolutions_seconds_unique', 'seconds', unique=True),
    )

    id: str = Field(sa_column=Column('id', CHAR(36), primary_key=True))
    status: str = Field(sa_column=Column('status', String(255), nullable=False, server_default=text("'ACTIVE'")))
    display_name: Optional[str] = Field(default=None, sa_column=Column('display_name', String(255)))
    seconds: Optional[int] = Field(default=None, sa_column=Column('seconds', Integer))

    providers_resolutions: list['ProvidersResolution'] = Relationship(back_populates='resolution')


class Asset(SQLModel, table=True):
    __tablename__ = 'assets'
    __table_args__ = (
        ForeignKeyConstraint(['provider'], ['providers.id'], ondelete='SET NULL', name='assets_provider_foreign'),
        Index('assets_provider_foreign', 'provider')
    )

    id: str = Field(sa_column=Column('id', CHAR(36), primary_key=True))
    status: str = Field(sa_column=Column('status', String(255), nullable=False, server_default=text("'ACTIVE'")))
    display_name: Optional[str] = Field(default=None, sa_column=Column('display_name', String(255)))
    base: Optional[str] = Field(default=None, sa_column=Column('base', String(255)))
    quote: Optional[str] = Field(default=None, sa_column=Column('quote', String(255)))
    first_date: Optional[datetime.datetime] = Field(default=None, sa_column=Column('first_date', DateTime))
    last_date: Optional[datetime.datetime] = Field(default=None, sa_column=Column('last_date', DateTime))
    provider_id: Optional[str] = Field(default=None, sa_column=Column('provider', CHAR(36)))
    symbol: Optional[str] = Field(default=None, sa_column=Column('symbol', String(255)))

    provider: Optional['Provider'] = Relationship(back_populates='assets')


class ProvidersResolution(SQLModel, table=True):
    __tablename__ = 'providers_resolutions'
    __table_args__ = (
        ForeignKeyConstraint(['providers_id'], ['providers.id'], ondelete='SET NULL', name='providers_resolutions_providers_id_foreign'),
        ForeignKeyConstraint(['resolutions_id'], ['resolutions.id'], ondelete='SET NULL', name='providers_resolutions_resolutions_id_foreign'),
        Index('providers_resolutions_providers_id_foreign', 'providers_id'),
        Index('providers_resolutions_resolutions_id_foreign', 'resolutions_id')
    )

    id: int = Field(sa_column=Column('id', INTEGER, primary_key=True))
    providers_id: Optional[str] = Field(default=None, sa_column=Column('providers_id', CHAR(36)))
    resolutions_id: Optional[str] = Field(default=None, sa_column=Column('resolutions_id', CHAR(36)))
    resolution_seconds: Optional[int] = Field(default=None, sa_column=Column('resolution_seconds', Integer))

    provider: Optional['Provider'] = Relationship(back_populates='providers_resolutions')
    resolution: Optional['Resolution'] = Relationship(back_populates='providers_resolutions')
