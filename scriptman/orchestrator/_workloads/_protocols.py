"""⚙️ Shared workload contracts for Orchestrator collaborators."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Mapping,
    Protocol,
    TypedDict,
    TypeVar,
    runtime_checkable,
)

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


@runtime_checkable
class WorkloadEntry(Protocol):
    """🧾 Unit of work managed by the Orchestrator."""

    @property
    def name(self) -> str:
        """🔍 Logical name for UI logs and lookups."""
        ...

    @property
    def kind(self) -> WorkloadKind:
        """🔍 Workload category (task, service, schedule)."""
        ...

    @property
    def metadata(self) -> Mapping[str, Any]:
        """🔍 Arbitrary metadata supplied by the façade."""
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


class WorkloadResult(Protocol[EntryTypeOutput]):
    """✅ Standard result contract emitted by workload executors."""

    @property
    def entry(self) -> EntryTypeOutput:
        """🔍 Descriptor that originated this execution."""
        ...

    @property
    def outcome(self) -> WorkloadOutcome:
        """🔍 Outcome status reported by executors."""
        ...

    @property
    def started_at(self) -> datetime:
        """⏱️ Start timestamp for the workload."""
        ...

    @property
    def finished_at(self) -> datetime:
        """⏱️ Finish timestamp for the workload."""
        ...

    @property
    def detail(self) -> Mapping[str, Any]:
        """🔍 Extra result details (duration, retries, etc.)."""
        ...

    @property
    def error(self) -> BaseException | None:
        """🔍 Captured error when the workload fails."""
        ...


class WorkloadExecutor(Protocol[EntryTypeInput, ResultTypeOutput]):
    """⚙️ Executes a workload entry and returns a result."""

    def execute(
        self,
        entry: EntryTypeInput,
        *,
        context: RuntimeContext,
    ) -> ResultTypeOutput:
        """🚀 Perform the workload and return a typed result."""
        ...


class WorkloadSummaryReporter(Protocol[ResultTypeInput]):
    """📊 Emits summaries after workload execution."""

    def report(self, result: ResultTypeInput, *, context: RuntimeContext) -> None:
        """🗒️ Publish a summary for operators."""
        ...


class WorkloadRegistry(Protocol[EntryType]):
    """🗂️ Persistent store for workload entries."""

    def add(self, entry: EntryType) -> None:
        """✍️ Register an entry for future lookups."""
        ...

    def get(self, name: str) -> EntryType:
        """🔍 Retrieve an entry by name."""
        ...


class WorkloadQueue(Protocol[EntryType]):
    """🧱 Queue abstraction used prior to execution."""

    def enqueue(
        self,
        entry: EntryType,
        *,
        context: RuntimeContext,
    ) -> str:
        """🚀 Enqueue a workload entry and return a task identifier."""
        ...

    def dequeue(
        self,
        *,
        context: RuntimeContext,
    ) -> tuple[str, EntryType] | None:
        """🔄 Retrieve the next entry to process, if available."""
        ...

    def complete(self, task_id: str, *, context: RuntimeContext) -> None:
        """✅ Mark a task identifier as completed."""
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
