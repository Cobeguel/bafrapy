import json

from dataclasses import dataclass, field
from typing import Generic, List, TypeVar

from loguru import logger
from peewee import *
from peewee import DoesNotExist, Model

from bafrapy.env_reader.database import EnvReader
from bafrapy.libs.singleton import Singleton
from bafrapy.models.management.base import BaseModel
from bafrapy.models.management.providers import *

T = TypeVar('T', bound=Model)

class GenericRepository(Generic[T]):
    def __init__(self, model: T):
        self.model = model

    def create(self, **kwargs) -> T:
        return self.model.create(**kwargs)

    def get_by_id(self, id) -> T:
        try:
            return self.model.get_by_id(id)
        except DoesNotExist:
            return None

    def update(self, id, **kwargs) -> T:
        try:
            obj = self.model.get_by_id(id)
            for key, value in kwargs.items():
                setattr(obj, key, value)
            obj.save()
            return obj
        except DoesNotExist:
            return None

    def delete(self, id) -> bool:
        try:
            obj = self.model.get_by_id(id)
            obj.delete_instance()
            return True
        except DoesNotExist:
            return False

    def list(self, **filters) -> List[T]:
        if filters:
            return self.model.select().where(**filters).order_by(self.model.id)
        else:
            return self.model.select()
            

@dataclass
class ManagementRepository(metaclass=Singleton):
    db: MySQLDatabase = field(default=None, init=False)
    providers: GenericRepository = field(default=GenericRepository(Provider), init=False)
    resolutions: GenericRepository = field(default=GenericRepository(Resolution), init=False)
    
    def __post_init__(self):
        logger.info("Initializing Mysql client")
        self.db = MySQLDatabase(
            database=EnvReader().MYSQL_DATABASE,
            user=EnvReader().MYSQL_USER,
            password=EnvReader().MYSQL_PASSWORD,
            host='localhost',
            port=3306)
        
        self.db.connect()
        BaseModel._meta.database.initialize(self.db)
        logger.info("Mysql client initialized")
        self.init_repo()        

    def __del__(self):
        self.db.close()

    def _init_tables(self):
        logger.info("Creating tables")
        self.db.create_tables([Provider, Resolution, ProviderResolution])


    def init_repo(self):
        logger.info("Initializing data")
        self._init_tables()

        with open('bafrapy/init_data/models.json', 'r') as f:
            setup = json.load(f)
        
        with self.db.atomic():
            for resolution in setup['resolutions']:
                resolution = Resolution.get_or_create(
                    time_seconds=resolution['time_seconds'],
                    name=resolution['name'], code=resolution['code'])

            resolutions = (Resolution.select())
            for provider in setup['providers']:
                p, _ = Provider.get_or_create(name=provider['name'])
                for resolution in provider['resolutions']:
                    r = next((r for r in resolutions if r.code == resolution), None)
                    if r is None:
                        raise ValueError(f"Resolution {resolution} not found")
                    ProviderResolution.get_or_create(provider=p.id, resolution=r.id)
