from bafrapy.models import Asset
from bafrapy.models.providers import Provider
from tests.models.base import IntegrationTestBase


class TestProviderIntegration(IntegrationTestBase):

    def test_insert_provider(self):
        provider = Provider(id="BINANCE", display_name="Binance")

        with self.main_repo.start_session() as uow:
            uow.providers.save(provider)
            uow.commit()

        with self.main_repo.start_session() as uow:
            result = uow.providers.get_by_id("BINANCE")

        assert result is not None
        assert result.id == "BINANCE"
        assert result.display_name == "Binance"

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

    