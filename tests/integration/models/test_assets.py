from bafrapy.models import Asset, Provider
from uuid import uuid4

from tests.integration.base import IntegrationTestDB


class TestAssetIntegration(IntegrationTestDB):

    def test_create_asset(self):
        provider = Provider(id="BINANCE", display_name="Binance")
        id = str(uuid4())
        asset = Asset(id=id, provider=provider, symbol="BTC", base="BTC", quote="USDT")

        with self.main_repo.start_session() as uow:
            uow.providers.save(provider)
            uow.assets.save(asset)
            uow.commit()

        with self.main_repo.start_session() as uow:
            result = uow.assets.get_by_id(id)

            assert result is not None
            assert result.provider.id == "BINANCE"
            assert result.id == id
            assert result.symbol == "BTC"
            assert result.base == "BTC"
            assert result.quote == "USDT"

    def test_archive_provider(self):
        provider = Provider(id="BINANCE", display_name="Binance")

        with self.main_repo.start_session() as uow:
            uow.providers.save(provider)
            uow.commit()

        with self.main_repo.start_session() as uow:
            uow.providers.archive(provider)
            uow.commit()

        with self.main_repo.start_session() as uow:
            result_filtered = uow.providers.get_by_id("BINANCE")
            result_archived = uow.providers.get_by_id("BINANCE", archived=True)

        assert result_filtered is None
        assert result_archived is not None
        assert result_archived.status == "ARCHIVED"
        assert result_archived.display_name == "Binance"