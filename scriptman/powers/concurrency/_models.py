from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Generic, Optional, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from scriptman.powers.generics import T


class TaskStatus(str, Enum):
    """🎭 Task statuses for better lifecycle management"""

    FAILED = "failed"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class TaskResult(BaseModel, Generic[T]):
    """
    🚀 TaskResult Model - Encapsulates the result of a task execution

    This model stores the status, result, and duration of task executions.
    """

    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[T] = None
    error: Optional[BaseException] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[timedelta] = None
    parent_id: Optional[str] = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def calculate_duration(self) -> Self:
        """
        🌟 Duration Calculator - Automatically calculates the task duration during
        initialization if `start_time` and `end_time` are provided.
        """
        if self.start_time and self.end_time:
            self.duration = self.end_time - self.start_time
        return self


class BatchResult(BaseModel, Generic[T]):
    """
    📦 BatchResult Model - Encapsulates results for a batch of tasks
    """

    batch_id: str
    tasks: list[TaskResult[T]]
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    duration: Optional[timedelta] = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def calculate_duration(self) -> Self:
        """
        🌟 Duration Calculator - Automatically calculates the task duration
        during initialization if `start_time` and `end_time` are provided.
        """
        if self.start_time and self.end_time:
            self.duration = self.end_time - self.start_time
        return self

    @field_validator("tasks", mode="after")
    @classmethod
    def validate_tasks(cls, value: list[TaskResult[T]]) -> list[TaskResult[T]]:
        task_ids: set[str] = set()
        for task in value:
            if task.task_id in task_ids:
                raise ValueError(f"Duplicate task ID: {task.task_id}")
            task_ids.add(task.task_id)

        return value

    @property
    def completed(self) -> bool:
        """
        ✅ Completion Status - Checks if all tasks in the batch are completed

        Returns:
            bool: True if all tasks are completed, failed, or cancelled, else False.
        """
        return all(
            task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)
            for task in self.tasks
        )

    @property
    def successful(self) -> bool:
        """
        🏆 Success Status - Checks if all tasks in the batch completed successfully

        Returns:
            bool: True if all tasks have `COMPLETED` status, else False.
        """
        return all(task.status == TaskStatus.COMPLETED for task in self.tasks)

    @property
    def failed_tasks(self) -> list[TaskResult[T]]:
        """
        ❌ Failed Tasks - Gets a list of all failed tasks

        Returns:
            list[TaskResult[T]]: List of tasks with `FAILED` status.
        """
        return [task for task in self.tasks if task.status == TaskStatus.FAILED]

    @property
    def results(self) -> list[T]:
        """
        📦 Results - Gets a list of all task results

        Returns:
            list[T]: List of task results
        """
        return [task.result for task in self.tasks if task.result]

    @property
    def all_tasks(self) -> list[TaskResult[T]]:
        """
        📦 All Tasks - Gets a list of all tasks

        Returns:
            list[TaskResult[T]]: List of all tasks
        """
        return self.tasks

    @property
    def stats(self) -> dict[str, Any]:
        """
        🗃️ Stats - Provides a dictionary representation of the instance's statistics.

        Returns:
            dict: Batch details including ID, task counts, completion statuses, and
                execution timings.
        """
        return {
            "batch_id": self.batch_id,
            "total_tasks": len(self.tasks),
            "completed_tasks": sum(
                1 for t in self.tasks if t.status == TaskStatus.COMPLETED
            ),
            "failed_tasks": sum(1 for t in self.tasks if t.status == TaskStatus.FAILED),
            "cancelled_tasks": sum(
                1 for t in self.tasks if t.status == TaskStatus.CANCELLED
            ),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "tasks": {task.task_id: task.model_dump() for task in self.tasks},
        }
