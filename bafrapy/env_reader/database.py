from dataclasses import InitVar, dataclass, field

from environs import Env
from loguru import logger

from bafrapy.libs.singleton import Singleton


@dataclass
class EnvReader(metaclass=Singleton):
    path : InitVar[str] = field(default=".env")

    _env : Env = field(default=None, init=False)

    def __post_init__(self, path: str):
        env = Env()
        env.read_env(".env")
        self._env = env

    def _get_env(self, key: str) -> str:
        if self._env is None:
            raise ValueError("Environment variables not loaded")
        try:
            return self._env(key)
        except KeyError:
            logger.error(f"Environment variable {key} not found")
            raise

    @property
    def CH_USER(self) -> str:
        return self._get_env('CH_USER')
    
    @property
    def CH_PASSWORD(self) -> str:
        return self._get_env('CH_PASSWORD')

    @property
    def CH_DATABASE(self) -> str:
        return self._get_env('CH_DATABASE')

    @property
    def MYSQL_USER(self) -> str:
        return self._get_env('MYSQL_USER')
    
    @property
    def MYSQL_PASSWORD(self) -> str:
        return self._get_env('MYSQL_PASSWORD')

    @property
    def MYSQL_DATABASE(self) -> str:
        return self._get_env('MYSQL_DATABASE')
