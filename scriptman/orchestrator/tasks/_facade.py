from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Mapping, cast

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._events import RuntimeEventTopic, make_event
from scriptman.orchestrator.workloads import (
    WorkloadEventPayload,
    WorkloadExecutor,
    WorkloadKind,
    WorkloadOutcome,
    WorkloadQueue,
    WorkloadRegistry,
    WorkloadSummaryReporter,
)

TaskCallable = Callable[..., object]


@dataclass
class TaskDescriptor:
    """🧾 Immutable description of a registered task."""

    name: str
    target: TaskCallable
    metadata: Mapping[str, object] = field(default_factory=dict)

    @property
    def kind(self) -> WorkloadKind:
        """🔍 Workload kind identifier for Command Deck events."""
        return "task"


@dataclass
class TaskExecutionResult:
    """✅ Summary bundle returned after executing a task."""

    task_id: str
    descriptor: TaskDescriptor
    outcome: WorkloadOutcome
    started_at: datetime
    finished_at: datetime
    detail: Mapping[str, Any] = field(default_factory=dict)
    result: object | None = None
    error: BaseException | None = None

    @property
    def duration(self) -> float:
        """⏱️ Duration in seconds between start and finish."""
        return (self.finished_at - self.started_at).total_seconds()


TaskQueueAdapter = WorkloadQueue[TaskDescriptor]
TaskExecutor = WorkloadExecutor[TaskDescriptor, TaskExecutionResult]
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
        self.context = context
        self.registry = registry
        self.queue = queue
        self.executor = executor
        self.reporter = reporter

    def register(self, descriptor: TaskDescriptor) -> None:
        """✍️ Register a new task with the underlying registry.

        Args:
            descriptor: The task descriptor.

        Raises:
            RuntimeError: If the task descriptor is not found.
        """

        self.registry.add(descriptor)
        payload: WorkloadEventPayload = {
            "workload_kind": "task",
            "workload_name": descriptor.name,
            "metadata": descriptor.metadata,
        }
        self._emit(RuntimeEventTopic.TASK_REGISTERED, payload=payload)

    def enqueue(self, name: str) -> str:
        """
        🚀 Enqueue a registered task for background execution.

        Args:
            name: The name of the task.

        Returns:
            str: The task ID.

        Raises:
            RuntimeError: If the task descriptor is not found.
        """
        descriptor = self.registry.get(name)
        task_id = self.queue.enqueue(descriptor, context=self.context)
        payload: WorkloadEventPayload = {
            "task_id": task_id,
            "workload_kind": "task",
            "metadata": descriptor.metadata,
            "workload_name": descriptor.name,
        }
        self._emit(RuntimeEventTopic.TASK_ENQUEUED, payload=payload)
        return task_id

    def execute(
        self, *, task_id: str | None = None, name: str | None = None
    ) -> TaskExecutionResult:
        """
        🚀 Execute a task immediately, bypassing the queue when desired.

        Args:
            task_id: The task ID.
            name: The name of the task.

        Returns:
            TaskExecutionResult: The task execution result.

        Raises:
            RuntimeError: If the task descriptor is not found.
        """
        descriptor = self._resolve_descriptor(task_id=task_id, name=name)
        started_payload: WorkloadEventPayload = {
            "workload_kind": "task",
            "workload_name": descriptor.name,
            "metadata": descriptor.metadata,
            "status": "started",
        }
        self._emit(RuntimeEventTopic.TASK_STARTED, payload=started_payload)

        result = self.executor.execute(descriptor, context=self.context)

        completion_payload: WorkloadEventPayload = {
            "workload_kind": "task",
            "workload_name": descriptor.name,
            "metadata": descriptor.metadata,
            "detail": result.detail,
            "status": cast(str, result.outcome),
        }
        if result.error:
            completion_payload["error"] = str(result.error)
            self._emit(RuntimeEventTopic.TASK_FAILED, payload=completion_payload)
        else:
            self._emit(RuntimeEventTopic.TASK_COMPLETED, payload=completion_payload)

        self.reporter.report(result, context=self.context)
        return result

    def inspect(self, name: str) -> TaskDescriptor:
        """
        🔍 Inspect the configuration for a registered task.

        Args:
            name: The name of the task.

        Returns:
            TaskDescriptor: The task descriptor.

        Raises:
            RuntimeError: If the task descriptor is not found.
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
        event = make_event(
            topic,
            payload=payload,
            timestamp_factory=self.context.timestamp,
            **extras,
        )
        self.context.event_publisher.emit(event)

    def _resolve_descriptor(
        self,
        *,
        task_id: str | None,
        name: str | None,
    ) -> TaskDescriptor:
        """🔍 Resolve a task descriptor using either name or task identifier.

        Args:
            task_id: The task ID.
            name: The name of the task.

        Returns:
            TaskDescriptor: The task descriptor.

        Raises:
            RuntimeError: If the task descriptor is not found.
        """
        if name is not None:
            return self.registry.get(name)
        if task_id is not None:
            descriptor = self.registry.get_by_task_id(task_id)
            if descriptor is None:
                raise KeyError(f"⚠️ No task descriptor found for task_id {task_id!r}")
            return descriptor
        raise ValueError("⚠️ Either task_id or name must be provided")


class TaskRegistry(WorkloadRegistry[TaskDescriptor]):
    """🗂️ Contract for storing task descriptors."""

    def get_by_task_id(self, task_id: str) -> TaskDescriptor | None:
        """🔍 Retrieve a descriptor previously associated with a queue task ID.

        Args:
            task_id: The task ID.

        Returns:
            TaskDescriptor: The task descriptor.

        Raises:
            RuntimeError: If the task descriptor is not found.
        """
        ...
