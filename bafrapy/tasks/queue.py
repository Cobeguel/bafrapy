from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List

from attrs import define, field
from ksuid import ksuid
from rq import Queue as QueueRQ
from rq.job import Retry

from bafrapy.tasks.factory import JobCommand


@define
class SerializableTask(ABC):
    @abstractmethod
    def serialize(self) -> str:
        pass
    
    @abstractmethod
    def load(self, data: str):
        pass
    
    @abstractmethod
    def get_task_key(self) -> str:
        pass


@define
class TaskConfig:
    id_prefix: str = field(default="")
    timeout: int = field(default=180)
    max_retries: int = field(default=3)
    retry_interval: int = field(default=10)

    @timeout.validator
    def validate_timeout(self, timeout: int):
        if timeout < 0:
            raise ValueError("Timeout must be greater than 0")

    @max_retries.validator
    def validate_max_retries(self, max_retries: int):
        if max_retries < 0:
            raise ValueError("Max retries must be greater than 0")
        
    @retry_interval.validator
    def validate_retry_interval(self, retry_interval: int):
        if retry_interval < 0:
            raise ValueError("Retry interval must be greater than 0")


@define
class Queue():
    _queue: QueueRQ = field(alias="queue")
    default_config: TaskConfig = field(default=TaskConfig())
    _task_configs: Dict[str, TaskConfig] = field(factory=dict, init=False)

    def register_task_config(self, task_key: str, config: TaskConfig):
        self._task_configs[task_key] = config

    def enqueue_job(self, payload: SerializableTask):
        config = self._task_configs.get(payload.get_task_key(), self.default_config)
        if config.id_prefix != "":
            job_id = f"{config.id_prefix}/{str(ksuid())}"
        else:
            job_id = str(ksuid())
        self._queue.enqueue_call(
            JobCommand(payload.get_task_key(), payload.serialize()),
            job_id=job_id,
            timeout=config.timeout,
            retry=Retry(max=config.max_retries, interval=config.retry_interval))

    def get_pending_jobs(self) -> List[str]:
        return [job.id for job in self._queue.jobs]


# @define
# class TaskDispatcher():
#     _queues: Dict[QueueType, Queue] = field(factory=dict, init=False)

#     def register_queue(self, queue_type: QueueType, queue: Queue):
#         self._queues[queue_type] = queue

#     def get_pending_jobs(self, queue_type: QueueType) -> List[str]:
#         return self._queues[queue_type].get_pending_jobs()

#     def add_job(self, payload: SerializableTask):
#         self._queues[payload.get_task_queue()].enqueue_job(payload)
