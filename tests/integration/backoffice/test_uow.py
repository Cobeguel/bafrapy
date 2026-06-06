import pytest

from .base import BackofficeIntegrationTestDB


class TestUow(BackofficeIntegrationTestDB):
    def test_repos_available(self):
        with self.backoffice_repo.start_session() as uow:
            assert uow.exchanges is not None
            assert uow.markets is not None
            assert uow.resolutions is not None
            assert uow.market_availabilities is not None
            assert uow.tasks is not None

    def test_rollback_on_error(self):
        with pytest.raises(RuntimeError):
            with self.backoffice_repo.start_session() as uow:
                uow.exchanges.save(self.make_exchange())
                raise RuntimeError("test rollback on error")

        with self.backoffice_repo.start_session() as uow:
            result = uow.exchanges.get("binance")

            assert result is None

    def test_commit_persists(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.markets.save(self.make_market())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            exchange = uow.exchanges.get("binance")
            market = uow.markets.get("binance-BTCUSDT")

            assert exchange is not None
            assert market is not None
            assert market.exchange == "binance"
