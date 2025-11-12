from __future__ import annotations

from typing import Protocol

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._tasks._model import TaskExecutionResult
from scriptman.orchestrator._workloads import WorkloadSummaryReporter


class TaskReporter(WorkloadSummaryReporter[TaskExecutionResult], Protocol):
    """🗒️ Reporter contract specialised for tasks."""


class NoOpTaskReporter(WorkloadSummaryReporter[TaskExecutionResult]):
    """🔕 Reporter that deliberately does nothing (default)."""

    def report(self, result: TaskExecutionResult, *, context: RuntimeContext) -> None:
        """🔕 Report a task execution result.

        Args:
            result: The task execution result.
            context: The runtime context.
        """
        return None


__all__ = ["NoOpTaskReporter", "TaskReporter"]
