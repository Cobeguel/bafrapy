from typing import Dict
from uuid import uuid4

from attr import define

from bafrapy.logger import LogField, LoguruLogger as log
from bafrapy.models import Asset
from bafrapy.providers.base import ProviderClient
from bafrapy.repositories.mainrepository import MainRepository


@define
class SymbolTask:
    providers: Dict[str, ProviderClient]
    repository: MainRepository

    def run(self, providerName: str):
        try:
            provider_client = self.providers[providerName]
        except KeyError:
            raise ValueError(f"Provider {providerName} not found")
        
        provider_symbols = provider_client.list_available_symbols()
        log().info("Symbols fetched", LogField(key="count", value=len(provider_symbols)))

        with self.repository.start_session() as repo:
            provider = repo.providers.get_by_external_name(providerName)
            assets = repo.assets.get_by_provider(provider.id)
            assets_symbols = [a.symbol for a in assets]

            new_symbols = [s for s in provider_symbols if s.symbol not in assets_symbols]
            new_assets = [Asset (id=str(uuid4()), provider=provider, symbol=s.symbol, base=s.base, quote=s.quote ) for s in new_symbols]
            log().info("Adding new assets to repository", LogField(key="assets", value=len(new_assets)))
            repo.assets.save_all(new_assets)
            repo.commit()

            undated_assets_count = repo.assets.count_undated_assets(provider.id)
            log().info("Undated assets count", LogField(key="count", value=undated_assets_count))
            for i in range(0, undated_assets_count, 100):
                log().info("Updating undated assets", LogField(key="count", value=i))

                undated_assets = repo.assets.get_undated_assets(provider.id, limit=100)
                for asset in undated_assets:
                    if asset.symbol is None:
                        log().warning("Asset has no symbol. Will not be updated", LogField(key="asset", value=asset.id))
                        continue

                    first_date = provider_client.symbol_first_date(asset.symbol)
                    if first_date:
                        asset.first_date = first_date
                
                repo.commit()
                log().info("Processed symbol batch", LogField(key="count", value=len(undated_assets)))
            
        log().info("Sync task completed", LogField(key="provider", value=providerName))