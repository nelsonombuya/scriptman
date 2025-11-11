from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._events import RuntimeEventTopic, make_event

TaskCallable = Callable[..., object]


@dataclass(slots=True)
class TaskDescriptor:
    """🧾 Immutable description of a registered task."""

    name: str
    target: TaskCallable
    metadata: Mapping[str, object]


@dataclass(slots=True)
class TaskExecutionResult:
    """✅ Summary bundle returned after executing a task."""

    task_id: str
    descriptor: TaskDescriptor
    outcome: str
    duration_seconds: float
    result: object | None = None
    error: BaseException | None = None


class TaskQueueAdapter(Protocol):
    """📦 Interface for queueing work prior to execution."""

    def enqueue(self, descriptor: TaskDescriptor, *, context: RuntimeContext) -> str:
        """🚀 Enqueue a task for background execution.

        Args:
            descriptor: The task descriptor.
            context: The runtime context.

        Returns:
            str: The task ID.
        """
        ...

    def dequeue(self, *, context: RuntimeContext) -> tuple[str, TaskDescriptor] | None:
        """🔄 Dequeue a task for immediate execution.

        Args:
            context: The runtime context.

        Returns:
            tuple[str, TaskDescriptor] | None: The task ID and descriptor.
        """
        ...

    def complete(self, task_id: str, *, context: RuntimeContext) -> None:
        """🏁 Complete a task after execution.

        Args:
            task_id: The task ID.
            context: The runtime context.

        Raises:
            RuntimeError: If the task is not found.
        """
        ...


class TaskExecutor(Protocol):
    """⚙️ Strategy object responsible for executing task callables."""

    def execute(
        self,
        descriptor: TaskDescriptor,
        *,
        context: RuntimeContext,
    ) -> TaskExecutionResult:
        """🚀 Execute a task.

        Args:
            descriptor: The task descriptor.
            context: The runtime context.

        Returns:
            TaskExecutionResult: The task execution result.
        """
        ...


class TaskSummaryReporter(Protocol):
    """📊 Collect and emit summaries after execution concludes."""

    def report(self, result: TaskExecutionResult, *, context: RuntimeContext) -> None:
        """📊 Report the result of a task execution.

        Args:
            result: The task execution result.
            context: The runtime context.

        Returns:
            None: The task execution result is reported.

        Raises:
            RuntimeError: If the task execution result is not found.
        """
        ...


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
        self._emit(RuntimeEventTopic.TASK_REGISTERED, name=descriptor.name)

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
        self._emit(RuntimeEventTopic.TASK_ENQUEUED, task_id=task_id, name=name)
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
        self._emit(RuntimeEventTopic.TASK_STARTED, name=descriptor.name)
        result = self.executor.execute(descriptor, context=self.context)
        if result.error:
            self._emit(RuntimeEventTopic.TASK_FAILED, name=descriptor.name)
        else:
            self._emit(RuntimeEventTopic.TASK_COMPLETED, name=descriptor.name)
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

    def _emit(self, topic: RuntimeEventTopic, **payload: object) -> None:
        """🔍 Emit an event.

        Args:
            topic: The topic of the event.
            **payload: The payload of the event.

        Raises:
            RuntimeError: If the event is not found.
        """
        event = make_event(
            topic,
            timestamp_factory=self.context.timestamp,
            **payload,
        )
        self.context.event_publisher.emit(event)

    def _resolve_descriptor(
        self, *, task_id: str | None, name: str | None
    ) -> TaskDescriptor:
        """🔍 Resolve a task descriptor.

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


class TaskRegistry(Protocol):
    """🗂️ Contract for storing task descriptors."""

    def add(self, descriptor: TaskDescriptor) -> None:
        """🗂️ Add a task descriptor to the registry.

        Args:
            descriptor: The task descriptor.

        Raises:
            RuntimeError: If the task descriptor is not found.
        """

    def get(self, name: str) -> TaskDescriptor:
        """🔍 Get a task descriptor by name.

        Args:
            name: The name of the task.

        Raises:
            RuntimeError: If the task descriptor is not found.
        """
        ...

    def get_by_task_id(self, task_id: str) -> TaskDescriptor | None:
        """🔍 Get a task descriptor by task ID.

        Args:
            task_id: The task ID.

        Returns:
            TaskDescriptor | None: The task descriptor.

        Raises:
            RuntimeError: If the task descriptor is not found.
        """
        ...
