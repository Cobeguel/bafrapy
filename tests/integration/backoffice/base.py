import uuid

from sqlalchemy import create_engine
from testcontainers.postgres import PostgresContainer

from bafrapy.backoffice.db.main_repository import BackofficeRepositoryBuilder
from bafrapy.backoffice.models import Exchange, Market, MarketAvailability, Resolution, Task, TaskStatus
from bafrapy.backoffice.models.base import Base
from bafrapy.logger.log import LoguruLogger as log


class BackofficeIntegrationTestDB:
    def setup_method(self):
        log().deactivate()
        self.container = PostgresContainer("postgres:17")
        self.container.start()
        self.dsn = self.container.get_connection_url(driver="psycopg")
        self.engine = create_engine(self.dsn)
        Base.metadata.create_all(self.engine)
        self.backoffice_repo = BackofficeRepositoryBuilder(dsn=self.dsn).build()

    def teardown_method(self):
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()
        self.container.stop()

    def make_exchange(self, id: str = "binance", display_name: str = "Binance", **kwargs) -> Exchange:
        return Exchange(id=id, display_name=display_name, **kwargs)

    def make_market(
        self,
        base: str = "BTC",
        quote: str = "USDT",
        symbol: str | None = None,
        raw_symbol: str | None = None,
        exchange: str = "binance",
        **kwargs,
    ) -> Market:
        return Market(
            symbol=symbol or f"{base}{quote}",
            raw_symbol=raw_symbol or f"{base}/{quote}",
            base=base,
            quote=quote,
            exchange=exchange,
            **kwargs,
        )

    def make_resolution(self, id: str = "1m", code: str = "1m", seconds: int = 60, **kwargs) -> Resolution:
        return Resolution(id=id, code=code, seconds=seconds, **kwargs)

    def make_market_availability(
        self,
        market: str = "binance-BTCUSDT",
        resolution: str = "1m",
        resolution_seconds: int = 60,
        **kwargs,
    ) -> MarketAvailability:
        return MarketAvailability(
            market=market,
            resolution=resolution,
            resolution_seconds=resolution_seconds,
            **kwargs,
        )

    def make_task(
        self,
        id: uuid.UUID | None = None,
        description: str = "sync ohlcv",
        status: TaskStatus = TaskStatus.RUNNING,
        **kwargs,
    ) -> Task:
        return Task(
            id=id or uuid.uuid4(),
            description=description,
            status=status,
            **kwargs,
        )
