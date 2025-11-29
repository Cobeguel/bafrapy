from bafrapy.models import Provider, Resolution
from tests.integration.base import IntegrationTestDB


class TestProviderIntegration(IntegrationTestDB):
    def test_insert_provider(self):
        provider = Provider(
            id="BINANCE", display_name="Binance", external_name="BINANCE"
        )

        with self.main_repo.start_session() as uow:
            uow.providers.save(provider)
            uow.commit()

        with self.main_repo.start_session() as uow:
            result = uow.providers.get_by_id("BINANCE")

            assert result is not None
            assert result.external_name == "BINANCE"
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

    def test_add_resolution_to_provider(self):
        provider = Provider(id="BINANCE", display_name="Binance")
        resolution = Resolution(id="600", display_name="1 minute", seconds=600)
        provider.resolutions.append(resolution)

        with self.main_repo.start_session() as uow:
            uow.providers.save(provider)
            uow.commit()

        with self.main_repo.start_session() as uow:
            result = uow.providers.get_by_id("BINANCE")
            assert result is not None
            assert len(result.resolutions) == 1
            assert result.resolutions[0].id == "600"
            assert result.resolutions[0].display_name == "1 minute"
            assert result.resolutions[0].seconds == 600
