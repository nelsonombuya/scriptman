from collections import deque
from dataclasses import dataclass
from threading import Lock
from time import time
from typing import Any, Deque
from uuid import uuid4

from scriptman.powers.tasks._execution_manager import ExecutorType


@dataclass
class QueueRecord:
    task_id: str
    func_path: str  # "module:qualname"
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    executor: ExecutorType
    enqueued_at: float
    retries: int = 0


class QueueManager:
    """
    ⛓️ In-memory queue used by TaskManager.

    This lightweight queue keeps pending and processing tasks entirely in memory.
    All state is lost when the process exits, matching the application's
    restart semantics: problematic tasks are not resurrected automatically.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._pending: Deque[QueueRecord] = deque[QueueRecord]()
        self._processing: Deque[QueueRecord] = deque[QueueRecord]()

    def enqueue(
        self,
        func_path: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        executor: ExecutorType = ExecutorType.THREAD,
    ) -> str:
        """
        🚀 Enqueue a task into the pending queue.

        Args:
            func_path: The path to the function to execute.
            args: The positional arguments to pass to the function.
            kwargs: The keyword arguments to pass to the function.
            executor: The executor to use to execute the function.

        Returns:
            str: The task ID.
        """
        record = QueueRecord(
            task_id=str(uuid4()),
            func_path=func_path,
            args=args,
            kwargs=kwargs,
            executor=executor,
            enqueued_at=time(),
        )
        with self._lock:
            self._pending.append(record)
        return record.task_id

    def dequeue(self) -> QueueRecord | None:
        """
        🔄 Dequeue a task from the pending queue and move it to the processing queue.

        Returns:
            QueueRecord | None: The task record or None if the queue is empty.
        """
        with self._lock:
            if not self._pending:
                return None
            record = self._pending.popleft()
            self._processing.append(record)
        return record

    def complete(self, task_id: str) -> bool:
        """
        ✅ Complete a task by removing it from the processing queue.

        Args:
            task_id: The ID of the task to complete.

        Returns:
            bool: True if the task was completed, False otherwise.
        """
        moved = False
        with self._lock:
            new_processing: Deque[QueueRecord] = deque[QueueRecord]()
            while self._processing:
                rec = self._processing.popleft()
                if rec.task_id == task_id:
                    moved = True
                    continue
                new_processing.append(rec)
            self._processing = new_processing
        return moved

    def stats(self) -> dict[str, int]:
        """Return simple queue statistics."""
        with self._lock:
            pending = len(self._pending)
            processing = len(self._processing)
        return {"pending": pending, "processing": processing}

    def __str__(self) -> str:
        """Return a string representation of the queue."""
        with self._lock:
            return (
                f"QueueManager(pending={len(self._pending)}, "
                f"processing={len(self._processing)}, "
                f"total={len(self._pending) + len(self._processing)})"
            )


__all__: list[str] = ["QueueManager", "QueueRecord", "ExecutorType"]
