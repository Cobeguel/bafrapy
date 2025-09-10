from abc import ABC, abstractmethod


class Worker:
    def execute_task(self, task_type: str, data: dict):
        task = TaskFactoryRegister.create_task(task_type, data)
        task.run()


def execute_task(task_type: str, data: dict):
    task = TaskFactoryRegister.create_task(task_type, data)
    task.run()


class RunnableTask(ABC):
    @abstractmethod
    def run(self):
        pass


class Reconstructable(ABC):
    @abstractmethod
    def build(self, data: dict) -> RunnableTask:
        pass


class TaskFactoryRegister():
    _registry: dict[str, Reconstructable] = {}

    @classmethod
    def register(cls, task_type: str, reconstructable: Reconstructable):
         cls._registry[task_type] = reconstructable

    @classmethod
    def create_task(cls, task_type: str, data: dict) -> RunnableTask:
        return cls._registry[task_type].build(data)


class JobCommand:
    def __init__(self, task_type: str, data: dict):
        self.task_type = task_type
        self.data = data

    def execute(self):
        task: RunnableTask = TaskFactoryRegister.create_task(self.task_type, self.data)
        task.run()

    def __call__(self):
        self.execute()