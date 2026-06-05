from typing import Callable

from attrs import define, field
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from bafrapy.backoffice.db.exchange_repository import ExchangeRepository
from bafrapy.backoffice.db.market_availability_repository import (
    MarketAvailabilityRepository,
)
from bafrapy.backoffice.db.market_repository import MarketRepository
from bafrapy.backoffice.db.resolution_repository import ResolutionRepository
from bafrapy.backoffice.db.task_repository import TaskRepository
from bafrapy.backoffice.db.uow import UnitOfWork
from bafrapy.logger import LogField, LoguruLogger as log


@define
class UnitOfWorkContext(UnitOfWork):
    _session: Session = field(alias="session")

    _exchanges: ExchangeRepository = field(init=False)
    _markets: MarketRepository = field(init=False)
    _resolutions: ResolutionRepository = field(init=False)
    _market_availabilities: MarketAvailabilityRepository = field(init=False)
    _tasks: TaskRepository = field(init=False)

    def __attrs_post_init__(self):
        self._exchanges = ExchangeRepository(self._session)
        self._markets = MarketRepository(self._session)
        self._resolutions = ResolutionRepository(self._session)
        self._market_availabilities = MarketAvailabilityRepository(self._session)
        self._tasks = TaskRepository(self._session)

    def __enter__(self) -> "UnitOfWorkContext":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self._session.rollback()

        self._session.close()

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    @property
    def exchanges(self) -> ExchangeRepository:
        return self._exchanges

    @property
    def markets(self) -> MarketRepository:
        return self._markets

    @property
    def resolutions(self) -> ResolutionRepository:
        return self._resolutions

    @property
    def market_availabilities(self) -> MarketAvailabilityRepository:
        return self._market_availabilities

    @property
    def tasks(self) -> TaskRepository:
        return self._tasks


@define
class BackofficeRepository:
    session_factory: Callable[[], Session]

    def start_session(self) -> UnitOfWorkContext:
        return UnitOfWorkContext(session=self.session_factory())


@define
class BackofficeRepositoryBuilder:
    dsn: str

    def build(self) -> BackofficeRepository:
        try:
            engine = create_engine(self.dsn)
            session_factory = sessionmaker(bind=engine)
            return BackofficeRepository(session_factory=session_factory)
        except Exception as e:
            log().error("Error building backoffice repository", LogField("error", e))
            raise
