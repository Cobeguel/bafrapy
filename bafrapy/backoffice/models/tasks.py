import datetime
import uuid

from enum import StrEnum
from typing import Optional

from sqlalchemy import DateTime, Enum as SAEnum, PrimaryKeyConstraint, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from bafrapy.backoffice.models.base import Base


class TaskStatus(StrEnum):
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (PrimaryKeyConstraint("id", name="tasks_pkey"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    start_date: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, default=None)
    end_date: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, default=None)
    description: Mapped[Optional[str]] = mapped_column(String(255), default=None)

    status: Mapped[TaskStatus] = mapped_column(
        SAEnum(TaskStatus, name="task_status_enum", create_type=False),
        nullable=False,
        default=TaskStatus.RUNNING,
    )
