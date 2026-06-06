import datetime

from .base import BackofficeIntegrationTestDB


class TestResolutionRepo(BackofficeIntegrationTestDB):
    def test_save_and_get(self):
        with self.backoffice_repo.start_session() as uow:
            uow.resolutions.save(self.make_resolution())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.resolutions.get("1m")

            assert result is not None
            assert result.id == "1m"
            assert result.code == "1m"
            assert result.seconds == 60

    def test_update(self):
        with self.backoffice_repo.start_session() as uow:
            uow.resolutions.save(self.make_resolution())
            uow.commit()

        updated_at = datetime.datetime(2025, 6, 1, 12, 0, 0)

        with self.backoffice_repo.start_session() as uow:
            stored = uow.resolutions.get("1m")
            stored.last_update = updated_at
            uow.resolutions.save(stored)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.resolutions.get("1m")

            assert result is not None
            assert result.last_update == updated_at

    def test_get_by_code(self):
        with self.backoffice_repo.start_session() as uow:
            uow.resolutions.save(self.make_resolution())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.resolutions.get_by_code("1m")

            assert result is not None
            assert result.id == "1m"

    def test_get_by_seconds(self):
        with self.backoffice_repo.start_session() as uow:
            uow.resolutions.save(self.make_resolution())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.resolutions.get_by_seconds(60)

            assert result is not None
            assert result.id == "1m"

    def test_exchange_resolution_relationship(self):
        exchange = self.make_exchange()
        resolution = self.make_resolution()

        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(exchange)
            uow.resolutions.save(resolution)
            exchange.resolutions.append(resolution)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            stored_exchange = uow.exchanges.get("binance")
            resolution_codes = [item.code for item in stored_exchange.resolutions]

            assert resolution_codes == ["1m"]
