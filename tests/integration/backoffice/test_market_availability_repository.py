import datetime

from .base import BackofficeIntegrationTestDB


class TestMarketAvailabilityRepo(BackofficeIntegrationTestDB):
    def test_save_and_get(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.markets.save(self.make_market())
            uow.resolutions.save(self.make_resolution())
            uow.market_availabilities.save(self.make_market_availability())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.market_availabilities.get("binance-BTCUSDT-60")

            assert result is not None
            assert result.market == "binance-BTCUSDT"
            assert result.resolution == "1m"

    def test_update(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.markets.save(self.make_market())
            uow.resolutions.save(self.make_resolution())
            uow.market_availabilities.save(self.make_market_availability())
            uow.commit()

        first_date = datetime.datetime(2024, 1, 1, 0, 0, 0)
        last_date = datetime.datetime(2024, 12, 31, 23, 59, 59)

        with self.backoffice_repo.start_session() as uow:
            stored = uow.market_availabilities.get("binance-BTCUSDT-60")
            stored.first_date = first_date
            stored.last_date = last_date
            uow.market_availabilities.save(stored)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.market_availabilities.get("binance-BTCUSDT-60")

            assert result is not None
            assert result.first_date == first_date
            assert result.last_date == last_date
