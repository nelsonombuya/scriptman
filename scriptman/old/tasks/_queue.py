from __future__ import annotations

from threading import Event, Thread
from typing import Callable, Generic

from scriptman.orchestrator import RuntimeContext
from scriptman.orchestrator._generics import P, R
from scriptman.orchestrator._tasks import (
    TaskExecutionResult,
    TasksFacade,
    TaskSubmission,
)
from scriptman.orchestrator._tasks._reporter import TaskReporter
from scriptman.orchestrator._workloads import WorkloadQueue


class QueueWorker(Generic[P, R], Thread):
    """🔄 Background worker that drains the task queue."""

    def __init__(
        self,
        *,
        facade: TasksFacade[P, R],
        queue: WorkloadQueue[TaskSubmission[P, R]],
        context: RuntimeContext,
        reporter: TaskReporter[P, R],
        on_complete: Callable[[str, TaskExecutionResult[P, R]], None],
    ) -> None:
        """🔄 Initialize the queue worker.

        Args:
            facade: The tasks facade.
            queue: The task queue.
            context: The runtime context.
            reporter: The task reporter.
            on_complete: The callback to call when a task is complete.
        """
        super().__init__(daemon=True, name="scriptman-queue-task-worker")
        self._wake = Event()
        self._stopped = Event()

        self._queue = queue
        self._facade = facade
        self._context = context
        self._reporter = reporter
        self._on_complete = on_complete

    def run(self) -> None:  # pragma: no cover - thin wrapper around facade
        """🔄 Run the queue worker."""
        while not self._stopped.is_set():
            item = self._queue.dequeue(context=self._context)
            if item is None:
                self._wake.wait(0.1)
                self._wake.clear()
                continue

            task_id, _submission = item
            try:
                result = self._facade.execute(task_id=task_id)
            except Exception as exc:  # pragma: no cover - defensive logging
                self._context.logger.error(
                    "⚠️ Task execution crashed", task_id=task_id, error=str(exc)
                )
                self._queue.complete(task_id, context=self._context)
                continue

            self._queue.complete(task_id, context=self._context)
            self._reporter.report(result, context=self._context)
            self._on_complete(task_id, result)

    def wake(self) -> None:
        """🔄 Wake the queue worker."""
        self._wake.set()

    def stop(self) -> None:
        """🔄 Stop the queue worker."""
        self._stopped.set()
        self._wake.set()
