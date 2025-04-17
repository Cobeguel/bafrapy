from dataclasses import dataclass, field
from enum import Enum
import os

import redis
from rq import Queue

from bafrapy.backend.tasks.worker import execute_task, JobCommand
from bafrapy.libs.singleton import Singleton
from typing import Protocol, runtime_checkable
from bafrapy.libs.serializable import Serializable

class QueueType(Enum):
    ASSETS = "assets"
    DATA = "data"
    BACKTEST = "backtest"


@runtime_checkable
class SerializableTask(Serializable, Protocol):
    def get_task_queue(self) -> QueueType:
        pass
    
    def get_task_key(self) -> str:
        pass


@dataclass
class TaskDispatcher():
    queues: dict[QueueType, Queue] = field(default_factory=dict, init=False)

    def register_queue(self, queue_type: QueueType, queue: Queue):
        self.queues[queue_type] = queue

    def add_job(self, payload: SerializableTask):
        self.queues[payload.get_task_queue()].enqueue_call(
            JobCommand(payload.get_task_key(), payload.serialize()))


