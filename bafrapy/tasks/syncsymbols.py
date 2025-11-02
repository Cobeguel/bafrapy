
import json

from attrs import define, field

from bafrapy.logger import LogField, LoguruLogger as log
from bafrapy.models import Asset
from bafrapy.providers.base import Provider
from bafrapy.repositories.mainrepository import MainRepository
from bafrapy.tasks.factory import Reconstructable, RunnableTask
from bafrapy.tasks.queue import SerializableTask


@define
class SyncSymbolsPayload(SerializableTask):
    _provider: str = field(alias="provider", default="")
    _batch_size: int = field(alias="batch_size", default=100)

    def serialize(self) -> str:
        return json.dumps({
            "provider": self._provider,
            "batch_size": self._batch_size,
        })
    
    def load(self, data: str):
        try:
            d = json.loads(data)
            self._provider = d["provider"]
            self._batch_size = d["batch_size"]
        except Exception as e:
            raise ValueError(f"Failed to load data: {e}")
    
    def get_task_key(self) -> str:
        return "sync_data"
    
    @property
    def provider(self) -> str:
        return self._provider

    @property
    def batch_size(self) -> int:
        return self._batch_size


@define
class SyncSymbolsTask(RunnableTask):
    _data: SyncSymbolsPayload = field(alias="data")
    _repository: MainRepository = field(alias="repository")
    _provider: Provider = field(alias="provider")
   
    def run(self):
        log().info("Starting sync task", LogField(key="provider", value=self._data.provider))
        provider_symbols = self._provider.list_available_symbols()
        log().info("Symbols fetched", LogField(key="count", value=len(provider_symbols)))

        with self._repository.start_session() as repo:
            provider = repo.providers.get_by_id(self._data.provider)
            assets = repo.assets.get_by_provider(self._data.provider)
            assets_symbols = [a.symbol for a in assets]

            new_symbols = [s for s in provider_symbols if s.symbol not in assets_symbols]
            new_assets = [Asset (provider, s.symbol, s.base, s.quote ) for s in new_symbols]
            log().info("Adding new assets to repository", LogField(key="assets", value=len(new_assets)))
            repo.assets.save_all(new_assets)
            repo.commit()

            undated_count = repo.assets.count_undated_assets(provider.id)
            log().info("Undated assets count", LogField(key="count", value=undated_count))
            for i in range(0, undated_count, self._data.batch_size):
                undated_assets = repo.assets.get_undated_assets(self._data.provider, limit=self._data.batch_size)
                for asset in undated_assets:
                    first_date = self._provider.symbol_first_date(asset.symbol)
                    if first_date:
                        asset.start_date = first_date
                
                log().info("Processed symbol batch", LogField(key="count", value=len(undated_assets)))

            repo.commit()

        log().info("Sync task completed", LogField(key="provider", value=self._data.provider))


class SyncSymbolsBuilder(Reconstructable):
    def build(self, data: dict) -> RunnableTask:
        return SyncSymbolsTask(SyncSymbolsPayload(data))