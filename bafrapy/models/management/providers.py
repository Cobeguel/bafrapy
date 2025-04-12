from dataclasses import dataclass, field
from uuid import uuid4

from peewee import *

from bafrapy.models.management.base import BaseModel


class Provider(BaseModel):
    id = CharField(primary_key=True, default=uuid4)
    name = CharField()


class Resolution(BaseModel):
    id = CharField(primary_key=True, default=uuid4)
    time_seconds = IntegerField(unique=True)
    name = CharField()
    code = CharField()


class ProviderResolution(BaseModel):
    id = CharField(primary_key=True, default=uuid4)
    provider = ForeignKeyField(Provider, backref='resolutions')
    resolution = ForeignKeyField(Resolution, backref='providers')

