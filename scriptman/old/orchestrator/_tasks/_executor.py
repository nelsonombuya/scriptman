from __future__ import annotations

from typing import Any, Generic, cast
from uuid import uuid4

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._generics import P, R
from scriptman.orchestrator._tasks._model import TaskExecutionResult, TaskSubmission
from scriptman.orchestrator._workloads import WorkloadExecutor, WorkloadOutcome


class SynchronousTaskExecutor(
    WorkloadExecutor[TaskSubmission[P, R], TaskExecutionResult[P, R]], Generic[P, R]
):
    """⚙️ Default executor that runs task callables in-process."""

    def execute(
        self,
        entry: TaskSubmission[P, R],
        *,
        context: RuntimeContext,
    ) -> TaskExecutionResult[P, R]:
        """🔄 Execute a task submission and return the execution result.

        Args:
            entry: The task submission.
            context: The runtime context.
        """
        started_at = context.timestamp()
        result_object: Any | None = None
        error: BaseException | None = None
        outcome: WorkloadOutcome = "success"
        entry.task_id = entry.task_id or f"{entry.name}-{uuid4()}"
        detail: dict[str, Any] = {"args": entry.args, "kwargs": entry.kwargs}

        try:
            result_object = self._invoke(entry)
        except BaseException as exc:  # pragma: no cover - propagated to caller
            outcome = "failure"
            error = exc

        finished_at = context.timestamp()
        return TaskExecutionResult(
            error=error,
            detail=detail,
            outcome=outcome,
            submission=entry,
            result=result_object,
            task_id=entry.task_id,
            started_at=started_at,
            finished_at=finished_at,
            config_generation=self._resolve_generation(context),
        )

    def _invoke(self, submission: TaskSubmission[P, R]) -> R:
        """🔄 Invoke the task submission and return the result.

        Args:
            submission: The task submission.
        """
        from asyncio import run
        from inspect import iscoroutinefunction

        target = submission.entry.target
        if iscoroutinefunction(target):
            return cast(R, run(target(*submission.args, **submission.kwargs)))
        return cast(R, target(*submission.args, **submission.kwargs))

    @staticmethod
    def _resolve_generation(context: RuntimeContext) -> str | None:
        """🔍 Resolve the config generation from the runtime context.

        Args:
            context: The runtime context.
        """
        generation = context.metadata.get("config_generation")
        if generation is None:
            return None
        return str(generation)


__all__ = ["SynchronousTaskExecutor"]
