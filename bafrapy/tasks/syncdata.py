import json

from attrs import define, field

from bafrapy.tasks.factory import Reconstructable, RunnableTask
from bafrapy.tasks.queue import SerializableTask


@define
class SyncDataPayload(SerializableTask):
    _provider: str = field(alias="provider", default="")
    _asset_type: str = field(alias="asset_type", default="")

    def serialize(self) -> str:
        return json.dumps({
            "provider": self._provider,
            "asset_type": self._asset_type,
        })
    
    def load(self, data: str):
        try:
            serialized = json.loads(data)
            self._provider = serialized["provider"]
            self._asset_type = serialized["asset_type"]
        except Exception as e:
            raise ValueError(f"Failed to load data: {e}")
    
    def get_task_key(self) -> str:
        return "sync_data"
    
    @property
    def provider(self) -> str:
        return self._provider
    
    @property
    def asset_type(self) -> str:
        return self._asset_type


@define
class SyncDataTask(RunnableTask):
    _data: SyncDataPayload = field(alias="data")

    def run(self):
        print(f"Running job with {self._data.provider} and {self._data.asset_type}")

class SyncDataBuilder(Reconstructable):
    def build(self, data: dict) -> RunnableTask:
        return SyncDataTask(SyncDataPayload(data))