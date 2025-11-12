from __future__ import annotations

from abc import ABC
from typing import Any, Mapping, cast
from uuid import uuid4

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._events import RuntimeEventTopic, make_event
from scriptman.orchestrator._logging import extract_logging_options, workload_log_sink
from scriptman.orchestrator.workloads import (
    WorkloadEventPayload,
    WorkloadExecutor,
    WorkloadQueue,
    WorkloadRegistry,
    WorkloadSummaryReporter,
)

from ._model import TaskEntry, TaskExecutionResult, TaskSubmission, default_task_logging

TaskQueueAdapter = WorkloadQueue[TaskSubmission]
TaskExecutor = WorkloadExecutor[TaskSubmission, TaskExecutionResult]
TaskSummaryReporter = WorkloadSummaryReporter[TaskExecutionResult]


class TasksFacade(ABC):
    """🚀 High-level API exposed to users via `TaskManager`."""

    def __init__(
        self,
        *,
        context: RuntimeContext,
        registry: "TaskRegistry",
        queue: TaskQueueAdapter,
        executor: TaskExecutor,
        reporter: TaskSummaryReporter,
    ) -> None:
        """🔄 Initialize the tasks facade.

        Args:
            context: The runtime context.
            registry: The task registry.
            queue: The task queue.
            executor: The task executor.
            reporter: The task reporter.
        """
        self.queue = queue
        self.context = context
        self.registry = registry
        self.executor = executor
        self.reporter = reporter

    def register(self, entry: TaskEntry) -> None:
        """✍️ Register a new task with the underlying registry.

        Args:
            entry: The task entry.

        Raises:
            RuntimeError: If the task entry is not found.
        """

        default_logging = default_task_logging()
        decorator_logging = extract_logging_options(entry.target)
        if entry.logging == default_logging and decorator_logging != default_logging:
            entry.logging = decorator_logging
        self.registry.add(entry)
        payload: WorkloadEventPayload = {
            "workload_kind": "task",
            "metadata": entry.metadata,
            "workload_name": entry.name,
        }
        self._emit(RuntimeEventTopic.TASK_REGISTERED, payload=payload)

    def enqueue(
        self,
        name: str,
        /,
        *args: Any,
        metadata: Mapping[str, object] | None = None,
        correlation_id: str | None = None,
        **kwargs: Any,
    ) -> str:
        """
        🚀 Enqueue a registered task for background execution.

        Args:
            name: The name of the task.

        Returns:
            str: The task ID.

        Raises:
            RuntimeError: If the task entry is not found.
        """

        entry = self.registry.get(name)
        submission = TaskSubmission(
            args=args,
            entry=entry,
            kwargs=dict(kwargs),
            extra_metadata=metadata or {},
            correlation_id=correlation_id,
        )
        task_id = self.queue.enqueue(submission, context=self.context)
        submission.task_id = task_id
        self.registry.remember_task_id(task_id, submission)
        payload: WorkloadEventPayload = {
            "task_id": task_id,
            "workload_kind": "task",
            "workload_name": entry.name,
            "metadata": submission.metadata,
        }
        if correlation_id:
            payload["detail"] = {"correlation_id": correlation_id}
        self._emit(RuntimeEventTopic.TASK_ENQUEUED, payload=payload)
        return task_id

    def execute(
        self,
        *,
        name: str | None = None,
        task_id: str | None = None,
        args: tuple[Any, ...] | None = None,
        kwargs: Mapping[str, Any] | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> TaskExecutionResult:
        """
        🚀 Execute a task immediately, bypassing the queue when desired.

        Args:
            name: The name of the task.
            task_id: The task ID.
            args: Positional arguments passed to the task when executing by name.
            kwargs: Keyword arguments passed to the task when executing by name.
            metadata: Additional metadata merged into the event payload.

        Returns:
            TaskExecutionResult: The task execution result.

        Raises:
            RuntimeError: If the task entry is not found.
        """
        submission = self._resolve_submission(
            name=name,
            task_id=task_id,
            args=args or (),
            kwargs=kwargs or {},
            metadata=metadata or {},
        )
        resolved_task_id = (
            task_id or submission.task_id or f"manual-{submission.entry.name}-{uuid4()}"
        )
        submission.task_id = resolved_task_id
        self.registry.remember_task_id(resolved_task_id, submission)

        started_payload: WorkloadEventPayload = {
            "status": "started",
            "workload_kind": "task",
            "task_id": resolved_task_id,
            "metadata": submission.metadata,
            "workload_name": submission.entry.name,
        }
        self._emit(RuntimeEventTopic.TASK_STARTED, payload=started_payload)

        with workload_log_sink(
            run_id=resolved_task_id,
            workload=submission.entry.name,
            options=submission.entry.logging,
        ):
            result = self.executor.execute(submission, context=self.context)
        if not getattr(result, "task_id", None):
            result.task_id = resolved_task_id

        completion_payload: WorkloadEventPayload = {
            "workload_kind": "task",
            "detail": result.detail,
            "task_id": resolved_task_id,
            "metadata": submission.metadata,
            "status": cast(str, result.outcome),
            "workload_name": submission.entry.name,
        }
        if result.error:
            completion_payload["error"] = str(result.error)
            self._emit(RuntimeEventTopic.TASK_FAILED, payload=completion_payload)
        else:
            self._emit(RuntimeEventTopic.TASK_COMPLETED, payload=completion_payload)

        self.reporter.report(result, context=self.context)
        if result.error is not None:
            raise result.error
        return result

    def inspect(self, name: str) -> TaskEntry:
        """
        🔍 Inspect the configuration for a registered task.

        Args:
            name: The name of the task.

        Returns:
            TaskEntry: The task entry.

        Raises:
            RuntimeError: If the task entry is not found.
        """
        return self.registry.get(name)

    def _emit(
        self,
        topic: RuntimeEventTopic,
        *,
        payload: WorkloadEventPayload | Mapping[str, object] | None = None,
        **extras: object,
    ) -> None:
        """
        📡 Emit an orchestrator event with merged payload data.

        Args:
            topic: The topic of the event.
            payload: The payload of the event.
            **extras: Additional key-value fields merged into the payload.

        Raises:
            RuntimeError: If the event is not found.
        """
        payload_data: dict[str, object] = {}
        if payload is not None:
            payload_data.update(dict(payload))
        if extras:
            payload_data.update(extras)
        event = make_event(
            topic,
            payload=payload_data,
            timestamp_factory=self.context.timestamp,
        )
        self.context.event_publisher.emit(event)

    def _resolve_submission(
        self,
        *,
        task_id: str | None,
        name: str | None,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        metadata: Mapping[str, object],
    ) -> TaskSubmission:
        """🔍 Resolve a task submission using either name or task identifier.

        Args:
            task_id: The task ID.
            name: The name of the task.
            args: Positional arguments for on-demand execution.
            kwargs: Keyword arguments for on-demand execution.
            metadata: Additional metadata for on-demand execution.

        Returns:
            TaskSubmission: The resolved task submission.

        Raises:
            RuntimeError: If the task entry is not found.
        """
        if name is None:
            if task_id is None:
                raise ValueError("⚠️ Either task_id or name must be provided")
            submission = self.registry.get_by_task_id(task_id)
            if submission is None:
                raise KeyError(f"⚠️ No task submission found for task_id {task_id!r}")
            return submission
        else:
            entry = self.registry.get(name)
            return TaskSubmission(
                entry=entry,
                args=args,
                kwargs=dict(kwargs),
                extra_metadata=metadata,
            )


class TaskRegistry(WorkloadRegistry[TaskEntry]):
    """🗂️ Contract for storing task entries and submissions."""

    def remember_task_id(self, task_id: str, submission: TaskSubmission) -> None:
        """🔄 Associate a queue-provided task ID with a submission."""
        ...

    def get_by_task_id(self, task_id: str) -> TaskSubmission | None:
        """🔍 Retrieve a submission previously associated with a queue task ID."""
        ...
