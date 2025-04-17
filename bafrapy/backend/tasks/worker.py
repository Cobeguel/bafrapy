from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable
from bafrapy.libs.singleton import Singleton
import rq

class Worker:
    def execute_task(self, task_type: str, data: dict):
        task = TaskFactory.create_task(task_type, data)
        task.run()

def execute_task(task_type: str, data: dict):
    task = TaskFactory.create_task(task_type, data)
    task.run()


@runtime_checkable
class RunnableTask(Protocol):
    def run(self):
        pass


@runtime_checkable
class Reconstructable(Protocol):
    def construct(self, data: dict) -> RunnableTask:
        pass


@dataclass
class TaskFactory(metaclass=Singleton):

    _registry: dict[str, Reconstructable] = field(default_factory=dict, init=False)

    def register(self, task_type: str, reconstructable: Reconstructable):
         self._registry[task_type] = reconstructable


    def create_task(self, task_type: str, data: dict) -> RunnableTask:
        return self._registry[task_type].construct(data)
    


class JobCommand:
    def __init__(self, task_type: str, data: dict):
        self.task_type = task_type
        self.data = data

    def run(self):
        task: RunnableTask = TaskFactory().create_task(self.task_type, self.data)
        task.run()

    def __call__(self):
        self.run()