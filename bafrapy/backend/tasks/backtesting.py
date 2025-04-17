
from dataclasses import dataclass, field, asdict
from bafrapy.backend.tasks.queues import QueueType
from bafrapy.backend.tasks.worker import RunnableTask, Reconstructable


@dataclass
class BacktetingPayload(RunnableTask):

    provider: str = field(default="")
    asset_type: str = field(default="")

    def get_task_queue(self) -> QueueType:
        return QueueType.BACKTEST
    
    def get_task_key(self) -> str:
        return "backtest"
    
    def serialize(self) -> dict:
        return asdict(self)
    
    def load(self, data: dict):
        self.provider = data["provider"]
        self.asset_type = data["asset_type"]

@dataclass
class BacktetingTask(RunnableTask):
    payload: BacktetingPayload
    
    def run(self):
        print(f"Running job with {self.provider} and {self.asset_type}")


class BacktetingBuilder(Reconstructable):
    def construct(self, data: dict) -> RunnableTask:
        return BacktetingTask(BacktetingPayload(data))

