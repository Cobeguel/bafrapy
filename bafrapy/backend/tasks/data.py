
from dataclasses import dataclass, field, asdict
from bafrapy.backend.tasks.queues import QueueType
from bafrapy.backend.tasks.worker import RunnableTask, Reconstructable

@dataclass
class SyncDataPayload(RunnableTask):

    provider: str = field(default="")
    asset_type: str = field(default="")

    def get_task_queue(self) -> QueueType:
        return QueueType.DATA
    
    def get_task_key(self) -> str:
        return "sync_data"
    
    def serialize(self) -> dict:
        return asdict(self)
    
    def load(self, data: dict):
        self.provider = data["provider"]
        self.asset_type = data["asset_type"]


@dataclass
class SyncDataTask(RunnableTask):
    data: SyncDataPayload = field(default_factory=SyncDataPayload)

    def run(self):
        print(f"Running job with {self.data.provider} and {self.data.asset_type}")

class SyncDataBuilder(Reconstructable):
    def construct(self, data: dict) -> RunnableTask:
        return SyncDataTask(SyncDataPayload(data))