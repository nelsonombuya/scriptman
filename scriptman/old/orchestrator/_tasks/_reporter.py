from __future__ import annotations

from abc import ABC

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._generics import P, R
from scriptman.orchestrator._tasks._model import TaskExecutionResult
from scriptman.orchestrator._workloads import WorkloadSummaryReporter


class TaskReporter(WorkloadSummaryReporter[TaskExecutionResult[P, R]], ABC):
    """🗒️ Reporter contract specialised for tasks.

    Extends WorkloadSummaryReporter with task-specific typing. Uses generics
    to maintain type relationships with TaskExecutionResult, enabling
    type-safe reporting operations.

    Runtime enforcement ensures all task reporters implement the report method,
    making summary emission consistent across task executions.
    """


class NoOpTaskReporter(WorkloadSummaryReporter[TaskExecutionResult[P, R]]):
    """🔕 Reporter that deliberately does nothing (default)."""

    def report(self, result: TaskExecutionResult[P, R], context: RuntimeContext) -> None:
        """🔕 Report a task execution result.

        Args:
            result: The task execution result.
            context: The runtime context.
        """
        return None


__all__ = ["NoOpTaskReporter", "TaskReporter"]
