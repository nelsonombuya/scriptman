"""⚙️ Shared workload contracts for Orchestrator collaborators."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, Mapping, TypedDict, TypeVar

from typing_extensions import NotRequired

if TYPE_CHECKING:
    from scriptman.orchestrator._context import RuntimeContext


# ⚙️ Workload type variables and constants
WorkloadKind = Literal[
    "task",
    "service",
    "schedule",
]
WorkloadOutcome = Literal[
    "success",
    "retry",
    "failure",
    "stopped",
    "running",
    "restarting",
]

# ⚙️ Workload entry type variables and constants
EntryType = TypeVar(
    "EntryType",
    bound="WorkloadEntry",
)
EntryTypeInput = TypeVar(
    "EntryTypeInput",
    bound="WorkloadEntry",
    contravariant=True,
)
EntryTypeOutput = TypeVar(
    "EntryTypeOutput",
    bound="WorkloadEntry",
    covariant=True,
)


class WorkloadEntry(ABC):
    """🧾 Unit of work managed by the Orchestrator.

    Defines the contract for all workload entries (tasks, services, schedules)
    with required properties for identification, categorization, and metadata.
    Implementations must provide these properties for proper orchestration.

    Runtime enforcement ensures incomplete implementations raise TypeError
    at instantiation, making contract violations immediately obvious.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """🔍 Logical name for UI logs and lookups.

        Returns:
            A unique identifier for this workload entry, used in logs,
            events, and user-facing interfaces.
        """
        ...

    @property
    @abstractmethod
    def kind(self) -> WorkloadKind:
        """🔍 Workload category (task, service, schedule).

        Returns:
            The workload kind, used for routing events and selecting
            appropriate executors and reporters.
        """
        ...

    @property
    @abstractmethod
    def metadata(self) -> Mapping[str, Any]:
        """🔍 Arbitrary metadata supplied by the façade.

        Returns:
            A mapping of metadata key-value pairs for extensibility.
            Used for filtering, tagging, and custom workload behaviors.
        """
        ...


# ⚙️ Workload result type variables and constants
ResultTypeOutput = TypeVar(
    "ResultTypeOutput",
    bound="WorkloadResult[Any]",
    covariant=True,
)
ResultTypeInput = TypeVar(
    "ResultTypeInput",
    bound="WorkloadResult[Any]",
    contravariant=True,
)


class WorkloadResult(ABC, Generic[EntryTypeOutput]):
    """✅ Standard result contract emitted by workload executors.

    Defines the complete execution report for any workload, including
    timing, outcome, errors, and links back to the originating entry.
    Uses generics to maintain type relationships through the result chain.

    Runtime enforcement ensures all required properties are implemented,
    preventing silent failures when accessing result data.
    """

    @property
    @abstractmethod
    def entry(self) -> EntryTypeOutput:
        """🔍 Entry that originated this execution.

        Returns:
            The workload entry that was executed, maintaining generic
            type information for type-safe access.
        """
        ...

    @property
    @abstractmethod
    def outcome(self) -> WorkloadOutcome:
        """🔍 Outcome status reported by executors.

        Returns:
            The execution outcome: success, retry, failure, stopped,
            running, or restarting. Used for routing and decision-making.
        """
        ...

    @property
    @abstractmethod
    def started_at(self) -> datetime:
        """⏱️ Start timestamp for the workload.

        Returns:
            The UTC timestamp when execution began. Used for duration
            calculations and timeline reconstruction.
        """
        ...

    @property
    @abstractmethod
    def finished_at(self) -> datetime:
        """⏱️ Finish timestamp for the workload.

        Returns:
            The UTC timestamp when execution completed or failed.
            Used with started_at to calculate duration.
        """
        ...

    @property
    @abstractmethod
    def detail(self) -> Mapping[str, Any]:
        """🔍 Extra result details (duration, retries, etc.).

        Returns:
            A mapping of additional execution details like duration,
            retry attempts, configuration generation, and custom metrics.
        """
        ...

    @property
    @abstractmethod
    def error(self) -> BaseException | None:
        """🔍 Captured error when the workload fails.

        Returns:
            The exception that caused failure, or None if execution
            succeeded. Used for error reporting and retry logic.
        """
        ...


class WorkloadExecutor(ABC, Generic[EntryTypeInput, ResultTypeOutput]):
    """⚙️ Executes a workload entry and returns a result.

    Defines the contract for all workload executors (task, service, schedule
    executors) that take entries and produce typed results. Uses generics to
    maintain type relationships between entry types and their corresponding results.

    Runtime enforcement ensures all executors implement the execute method,
    preventing silent failures when attempting to run workloads.
    """

    @abstractmethod
    def execute(
        self,
        entry: EntryTypeInput,
        *,
        context: RuntimeContext,
    ) -> ResultTypeOutput:
        """🚀 Perform the workload and return a typed result.

        Args:
            entry: The workload entry to execute.
            context: The runtime context providing config, logger, and event bus.

        Returns:
            A typed result containing execution outcome, timing, errors,
            and links back to the originating entry.

        Raises:
            BaseException: Executors may raise exceptions during execution,
                which should be captured in the result's error property.
        """
        ...


class WorkloadSummaryReporter(ABC, Generic[ResultTypeInput]):
    """📊 Emits summaries after workload execution.

    Defines the contract for reporters that consume execution results and
    emit summaries for operators, logs, or external systems. Uses generics
    to maintain type relationships with specific result types.

    Runtime enforcement ensures all reporters implement the report method,
    making summary emission consistent across workload types.
    """

    @abstractmethod
    def report(self, result: ResultTypeInput, *, context: RuntimeContext) -> None:
        """🗒️ Publish a summary for operators.

        Args:
            result: The execution result to summarize.
            context: The runtime context providing logger and event bus
                for emitting the summary.

        Implementations should emit logs, metrics, or events based on the
        result's outcome and details. May be a no-op for silent reporters.
        """
        ...


class WorkloadRegistry(ABC, Generic[EntryType]):
    """🗂️ Persistent store for workload entries.

    Defines the contract for registries that store and retrieve workload
    entries by name. Supports both simple name-based lookups and custom
    query mechanisms. Uses generics to maintain type relationships with
    specific entry types.

    Runtime enforcement ensures all registries implement add and get methods,
    preventing silent failures when registering or looking up workloads.
    """

    @abstractmethod
    def add(self, entry: EntryType) -> None:
        """✍️ Register an entry for future lookups.

        Args:
            entry: The workload entry to store. Must have a unique name
                within the registry.

        Raises:
            ValueError: If an entry with the same name already exists,
                unless the registry supports overwriting.
        """
        ...

    @abstractmethod
    def get(self, name: str) -> EntryType:
        """🔍 Retrieve an entry by name.

        Args:
            name: The unique name of the workload entry.

        Returns:
            The workload entry with the given name.

        Raises:
            KeyError: If no entry with the given name exists.
        """
        ...


class WorkloadQueue(ABC, Generic[EntryType]):
    """🧱 Queue abstraction used prior to execution.

    Defines the contract for queues that buffer workload entries before
    execution. Supports enqueueing, dequeuing, and completion tracking.
    Uses generics to maintain type relationships with specific entry types.

    Runtime enforcement ensures all queues implement enqueue, dequeue,
    and complete methods, making queue behavior predictable across
    implementations (in-memory, Redis, etc.).
    """

    @abstractmethod
    def enqueue(
        self,
        entry: EntryType,
        *,
        context: RuntimeContext,
    ) -> str:
        """🚀 Enqueue a workload entry and return a task identifier.

        Args:
            entry: The workload entry to queue for later execution.
            context: The runtime context for logging and event emission.

        Returns:
            A unique task identifier for this queued entry, used for
            tracking and completion marking.
        """
        ...

    @abstractmethod
    def dequeue(
        self,
        *,
        context: RuntimeContext,
    ) -> tuple[str, EntryType] | None:
        """🔄 Retrieve the next entry to process, if available.

        Args:
            context: The runtime context for logging and event emission.

        Returns:
            A tuple of (task_id, entry) if an entry is available,
            or None if the queue is empty.
        """
        ...

    @abstractmethod
    def complete(self, task_id: str, *, context: RuntimeContext) -> None:
        """✅ Mark a task identifier as completed.

        Args:
            task_id: The task identifier returned by enqueue.
            context: The runtime context for logging and event emission.

        Marks the task as completed, allowing cleanup of queue state.
        Should be idempotent (safe to call multiple times).
        """
        ...


class WorkloadEventPayload(TypedDict, total=False):
    """🛰️ Canonical payload shared with event subscribers."""

    workload_kind: WorkloadKind
    workload_name: str
    run_id: NotRequired[str]
    task_id: NotRequired[str]
    status: NotRequired[str]
    metadata: NotRequired[Mapping[str, Any]]
    detail: NotRequired[Mapping[str, Any]]
    error: NotRequired[str]


@dataclass(slots=True)
class BaseWorkloadEntry(WorkloadEntry):
    """🧾 Minimal workload entry implementation."""

    name: str
    kind: WorkloadKind
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BaseWorkloadResult(Generic[EntryType], WorkloadResult[EntryType]):
    """✅ Minimal result implementation with convenience helpers."""

    entry: EntryType
    outcome: WorkloadOutcome
    started_at: datetime
    finished_at: datetime
    detail: Mapping[str, Any] = field(default_factory=dict)
    error: BaseException | None = None

    @property
    def duration_seconds(self) -> float:
        """⏱️ Duration in seconds between start and finish."""
        return (self.finished_at - self.started_at).total_seconds()


__all__ = [
    "BaseWorkloadEntry",
    "BaseWorkloadResult",
    "WorkloadEntry",
    "WorkloadEventPayload",
    "WorkloadExecutor",
    "WorkloadKind",
    "WorkloadOutcome",
    "WorkloadQueue",
    "WorkloadRegistry",
    "WorkloadResult",
    "WorkloadSummaryReporter",
]
