import datetime

from bafrapy.backoffice.models import TaskStatus

from .base import BackofficeIntegrationTestDB


class TestTaskRepo(BackofficeIntegrationTestDB):
    def test_save_and_get(self):
        task = self.make_task(description="backfill binance")
        task_id = task.id

        with self.backoffice_repo.start_session() as uow:
            uow.tasks.save(task)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.tasks.get(task_id)

            assert result is not None
            assert result.id == task_id
            assert result.description == "backfill binance"
            assert result.status == TaskStatus.RUNNING
            assert result.start_date is None
            assert result.end_date is None

    def test_save_with_dates_and_status(self):
        start = datetime.datetime(2025, 1, 1, 12, 0, 0)
        end = datetime.datetime(2025, 1, 1, 13, 0, 0)
        task = self.make_task(
            description="completed sync",
            status=TaskStatus.SUCCEEDED,
            start_date=start,
            end_date=end,
        )
        task_id = task.id

        with self.backoffice_repo.start_session() as uow:
            uow.tasks.save(task)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.tasks.get(task_id)

            assert result is not None
            assert result.status == TaskStatus.SUCCEEDED
            assert result.start_date == start
            assert result.end_date == end

    def test_update(self):
        task = self.make_task(description="backfill binance")
        task_id = task.id

        with self.backoffice_repo.start_session() as uow:
            uow.tasks.save(task)
            uow.commit()

        end = datetime.datetime(2025, 1, 1, 13, 0, 0)

        with self.backoffice_repo.start_session() as uow:
            stored = uow.tasks.get(task_id)
            stored.status = TaskStatus.SUCCEEDED
            stored.end_date = end
            uow.tasks.save(stored)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.tasks.get(task_id)

            assert result is not None
            assert result.status == TaskStatus.SUCCEEDED
            assert result.end_date == end

    def test_list(self):
        task_a = self.make_task(description="task a")
        task_b = self.make_task(description="task b")
        task_ids = {task_a.id, task_b.id}

        with self.backoffice_repo.start_session() as uow:
            uow.tasks.save(task_a)
            uow.tasks.save(task_b)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.tasks.list()

            assert len(result) == 2
            assert {task.id for task in result} == task_ids

    def test_remove(self):
        task = self.make_task()
        task_id = task.id

        with self.backoffice_repo.start_session() as uow:
            uow.tasks.save(task)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            uow.tasks.remove(task_id)
            uow.commit()

        with self.backoffice_repo.start_session() as uow:
            result = uow.tasks.get(task_id)

            assert result is None
