from bafrapy.backoffice.models.exchange import Status

from .base import BackofficeIntegrationTestDB


class TestExchangeRepo(BackofficeIntegrationTestDB):
    def test_save_and_get(self):
        exchange = self.make_exchange(type="CEX")

        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(exchange)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.exchanges.get("binance")

        assert result is not None
        assert result.id == "binance"
        assert result.display_name == "Binance"
        assert result.type == "CEX"
        assert result.status == Status.ACTIVE

    def test_get_by_display_name(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.exchanges.get_by_display_name("Binance")

            assert result is not None
            assert result.id == "binance"

    def test_list(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange(id="binance", display_name="Binance"))
            uow.exchanges.save(self.make_exchange(id="kraken", display_name="Kraken"))
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.exchanges.list()

            assert len(result) == 2
            assert {exchange.id for exchange in result} == {"binance", "kraken"}

    def test_update(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            stored = uow.exchanges.get("binance")
            stored.status = Status.ARCHIVED
            uow.exchanges.save(stored)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.exchanges.get("binance")

            assert result is not None
            assert result.status == Status.ARCHIVED

    def test_remove(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.remove("binance")
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.exchanges.get("binance")

        assert result is None
