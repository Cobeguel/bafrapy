from typing import Callable

from attrs import define, field
from sqlalchemy.orm import Session

from bafrapy.repositories.asset import AssetRepository
from bafrapy.repositories.provider import ProviderRepository
from bafrapy.repositories.resolution import ResolutionRepository
from bafrapy.repositories.uow import UnitOfWork


@define
class UnitOfWorkContext(UnitOfWork):
    _session: Session = field(alias="session")

    _providers: ProviderRepository = field(init=False)
    _assets: AssetRepository = field(init=False)
    _resolutions: ResolutionRepository = field(init=False)

    def __attrs_post_init__(self):
        self._providers = ProviderRepository(self._session)
        self._assets = AssetRepository(self._session)
        self._resolutions = ResolutionRepository(self._session)
    def __enter__(self) -> "UnitOfWorkContext":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self._session.rollback()
        else:
            self._session.commit()
        self._session.close()

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    @property
    def providers(self) -> ProviderRepository:
        return self._providers

    @property
    def assets(self) -> AssetRepository:
        return self._assets
    
    @property
    def resolutions(self) -> ResolutionRepository:
        return self._resolutions


@define
class MainRepository:
    session_factory: Callable[[], Session]

    def start_session(self) -> UnitOfWorkContext:
        return UnitOfWorkContext(session=self.session_factory())