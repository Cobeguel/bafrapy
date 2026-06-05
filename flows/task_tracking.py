import uuid

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from bafrapy.backoffice.db.main_repository import BackofficeRepository
from bafrapy.backoffice.models import Task, TaskStatus


def _start_task(repository: BackofficeRepository, task_id: uuid.UUID, description: str) -> None:
    now = datetime.now(timezone.utc)
    with repository.start_session() as uow:
        task = uow.tasks.get(task_id)
        if task is None:
            task = Task(id=task_id, description=description)
        else:
            task.description = description

        task.status = TaskStatus.RUNNING
        task.start_date = now
        task.end_date = None
        uow.tasks.save(task)
        uow.commit()


def _finish_task(repository: BackofficeRepository, task_id: uuid.UUID, status: TaskStatus) -> None:
    now = datetime.now(timezone.utc)
    with repository.start_session() as uow:
        task = uow.tasks.get(task_id)
        if task is None:
            return

        task.status = status
        task.end_date = now
        uow.tasks.save(task)
        uow.commit()


@contextmanager
def track_dagster_task(
    repository: BackofficeRepository,
    *,
    run_id: str,
    description: str,
) -> Iterator[uuid.UUID]:
    task_id = uuid.UUID(run_id)
    _start_task(repository, task_id, description)
    try:
        yield task_id
    except Exception:
        _finish_task(repository, task_id, TaskStatus.FAILED)
        raise
    else:
        _finish_task(repository, task_id, TaskStatus.SUCCEEDED)
