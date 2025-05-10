import json
import sys

from abc import ABC, abstractmethod
from typing import Optional, TextIO

from attrs import define, field
from loguru import logger

from bafrapy.libs.singleton import Singleton


@define
class LogField():
    key: str
    value: str


class Logger(ABC):
    @abstractmethod
    def debug(self, msg: str, *fields: LogField):
        ...
    @abstractmethod
    def info(self, msg: str, *fields: LogField):
        ...
    @abstractmethod
    def warning(self, msg: str, *fields: LogField):
        ...
    @abstractmethod
    def error(self, msg: str, *fields: LogField):
        ...
    @abstractmethod
    def exception(self, msg: str, *fields: LogField):
        ...
    @abstractmethod
    def critical(self, msg: str, *fields: LogField):
        ...


@define
class LoguruConfig():
    sink: Optional[TextIO] = field(default=sys.stdout)
    level: Optional[str] = None
    format: Optional[str] = None
    enqueue: Optional[bool] = None
    backtrace: Optional[bool] = None
    diagnose: Optional[bool] = None
    colorize: Optional[bool] = None
    log_file: Optional[str] = None
    rotation: Optional[str] = None

    @classmethod
    def from_file(cls, file_path: str):
        with open(file_path, "r") as f:
            config = json.load(f)
            try:
                return cls(
                    sink=sys.stdout,
                    level=config["level"],
                    format=config["format"],
                    enqueue=config["enqueue"],
                    backtrace=config["backtrace"],
                    diagnose=config["diagnose"],
                    colorize=config["colorize"],
                    log_file=config["log_file"],
                    rotation=config["rotation"]
                )
            except KeyError as e:
                raise ValueError(f"Missing key: {e}")

def format_record(record):
    timestamp = record["time"].strftime("%Y/%m/%d %H:%M:%S")
    level = record["level"].name
    message = record["message"]
    extras = " ".join(f"{k}={v}" for k, v in record["extra"].items())
    return f"{timestamp} {level} {message} {extras}"


@define
class LoguruLogger(Logger, metaclass=Singleton):
    deactivated: bool = field(default=False)
    config: Optional[LoguruConfig] = field(default=None)

    def __apply_config(self):
        logger.remove()
        params = {}

        if self.config.sink is not None:
            for field_name in ("sink", "level", "format", "enqueue", "backtrace", "diagnose", "colorize"):
                value = getattr(self.config, field_name)
                if value is not None:
                    params[field_name] = value

        params["sink"] = sys.stdout

        logger.add(**params)

    def is_configured(self) -> bool:
        return self.config is not None

    def set_config(self, config: LoguruConfig):
        if self.is_configured():
            raise RuntimeError("LoguruLogger already configured.")
        self.config = config
        self.__apply_config()

    def change_level(self, level: str):
        if self.deactivated:
            raise RuntimeError("LoguruLogger is deactivated.")
        
        if self.is_configured():
            self.config.level = level
            self.__apply_config()
        else:
            logger.remove()
            logger.add(sys.stdout, level=level)

    def deactivate(self):
        logger.remove()
        self.deactivated = True

    def activate(self):
        if self.is_configured():
            self.__apply_config()
        else:
            logger.remove()
            logger.add(sys.stdout, level=self.config.level)

    def _to_bind_kwargs(self, *fields: LogField) -> dict:
        if fields is None:
            return {}
        raw = {f.key: f.value for f in fields}
        formatted = " ".join(f"{k}={v}" for k, v in raw.items())
        return {**raw, "formatted": formatted}

    def debug(self, msg: str, *fields: LogField):
        logger.bind(**self._to_bind_kwargs(*fields)).debug(msg)

    def info(self, msg: str, *fields: LogField):
        logger.bind(**self._to_bind_kwargs(*fields)).info(msg)

    def warning(self, msg: str, *fields: LogField):
        logger.bind(**self._to_bind_kwargs(*fields)).warning(msg)

    def error(self, msg: str, *fields: LogField):
        logger.bind(**self._to_bind_kwargs(*fields)).error(msg)

    def exception(self, msg: str, *fields: LogField):
        logger.bind(**self._to_bind_kwargs(*fields)).exception(msg)

    def critical(self, msg: str, *fields: LogField):
        logger.bind(**self._to_bind_kwargs(*fields)).critical(msg)
