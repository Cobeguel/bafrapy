
from dataclasses import asdict, dataclass, field

from bafrapy.tasks.factory import Reconstructable, RunnableTask


@dataclass
class BacktetingPayload(RunnableTask):

    provider: str = field(default="")
    asset_type: str = field(default="")
    
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
    def build(self, data: dict) -> RunnableTask:
        return BacktetingTask(BacktetingPayload(data))

