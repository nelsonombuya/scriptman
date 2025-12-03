from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Deque, Dict, Generic
from uuid import uuid4

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._generics import P, R
from scriptman.orchestrator._tasks._model import TaskSubmission
from scriptman.orchestrator._workloads import WorkloadQueue


class InMemoryTaskQueue(Generic[P, R], WorkloadQueue[TaskSubmission[P, R]]):
    """🧱 In-memory queue suitable for single-process development and testing."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._pending: Deque[TaskSubmission[P, R]] = deque[TaskSubmission[P, R]]()
        self._inflight: Dict[str, TaskSubmission[P, R]] = {}

    def enqueue(
        self,
        submission: TaskSubmission[P, R],
        *,
        context: RuntimeContext,
    ) -> str:
        """🚀 Enqueue a submission and return its task identifier.

        Args:
            submission: The task submission.
            context: The runtime context.
        """
        task_id = submission.task_id or f"{submission.name}-{uuid4()}"
        submission.task_id = task_id
        with self._lock:
            self._pending.append(submission)
        return task_id

    def dequeue(
        self,
        *,
        context: RuntimeContext,
    ) -> tuple[str, TaskSubmission[P, R]] | None:
        """🔄 Retrieve the next submission ready for execution.

        Args:
            context: The runtime context.
        """
        with self._lock:
            if not self._pending:
                return None
            submission = self._pending.popleft()
            assert submission.task_id is not None  # for type-checkers
            self._inflight[submission.task_id] = submission
            return submission.task_id, submission

    def complete(self, task_id: str, *, context: RuntimeContext) -> None:
        """✅ Mark a task identifier as completed.

        Args:
            task_id: The task ID.
            context: The runtime context.
        """
        with self._lock:
            self._inflight.pop(task_id, None)


__all__ = ["InMemoryTaskQueue"]
