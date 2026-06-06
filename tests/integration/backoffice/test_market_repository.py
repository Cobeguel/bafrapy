from bafrapy.backoffice.models.exchange import SyncStatus

from .base import BackofficeIntegrationTestDB


class TestMarketRepo(BackofficeIntegrationTestDB):
    def test_save_and_get(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.markets.save(self.make_market(base="BTC", quote="USDT"))
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.markets.get("binance-BTCUSDT")

            assert result is not None
            assert result.symbol == "BTCUSDT"
            assert result.exchange == "binance"
            assert result.base == "BTC"
            assert result.quote == "USDT"

    def test_update(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.markets.save(self.make_market())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            stored = uow.markets.get("binance-BTCUSDT")
            stored.sync_status = SyncStatus.SYNCED
            uow.markets.save(stored)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.markets.get("binance-BTCUSDT")

            assert result is not None
            assert result.sync_status == SyncStatus.SYNCED

    def test_get_by_exchange_and_symbol(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.markets.save(self.make_market())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.markets.get_by_exchange_and_symbol("binance", "BTCUSDT")

            assert result is not None
            assert result.id == "binance-BTCUSDT"

    def test_list_by_exchange(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.exchanges.save(self.make_exchange(id="kraken", display_name="Kraken"))
            uow.markets.save(self.make_market(base="BTC", quote="USDT", exchange="binance"))
            uow.markets.save(self.make_market(base="BTC", quote="USD", exchange="kraken"))
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.markets.list_by_exchange("binance")

            assert len(result) == 1
            assert result[0].symbol == "BTCUSDT"

    def test_remove(self):
        with self.backoffice_repo.start_session() as uow:
            uow.exchanges.save(self.make_exchange())
            uow.markets.save(self.make_market())
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            uow.markets.remove("binance-BTCUSDT")
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.markets.get("binance-BTCUSDT")

            assert result is None
