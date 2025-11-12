"""⚙️ Shared workload contracts for Command Deck collaborators."""

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

WorkloadKind = Literal["task", "service", "schedule"]
WorkloadOutcome = Literal[
    "success",
    "retry",
    "failure",
    "stopped",
    "running",
    "restarting",
]

DescriptorT = TypeVar("DescriptorT", bound="WorkloadDescriptor")
DescriptorT_co = TypeVar("DescriptorT_co", bound="WorkloadDescriptor", covariant=True)
DescriptorT_contra = TypeVar(
    "DescriptorT_contra", bound="WorkloadDescriptor", contravariant=True
)


@runtime_checkable
class WorkloadDescriptor(Protocol):
    """🧾 Describes a unit of work managed by the Command Deck."""

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


class WorkloadResult(Protocol[DescriptorT_co]):
    """✅ Standard result contract emitted by workload executors."""

    @property
    def descriptor(self) -> DescriptorT_co:
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


ResultT_co = TypeVar("ResultT_co", bound="WorkloadResult[Any]", covariant=True)
ResultT_contra = TypeVar(
    "ResultT_contra", bound="WorkloadResult[Any]", contravariant=True
)


class WorkloadExecutor(Protocol[DescriptorT_contra, ResultT_co]):
    """⚙️ Executes a descriptor and returns a result."""

    def execute(
        self,
        descriptor: DescriptorT_contra,
        *,
        context: RuntimeContext,
    ) -> ResultT_co:
        """🚀 Perform the workload and return a typed result."""
        ...


class WorkloadSummaryReporter(Protocol[ResultT_contra]):
    """📊 Emits summaries after workload execution."""

    def report(self, result: ResultT_contra, *, context: RuntimeContext) -> None:
        """🗒️ Publish a summary for operators."""
        ...


class WorkloadRegistry(Protocol[DescriptorT]):
    """🗂️ Persistent store for workload descriptors."""

    def add(self, descriptor: DescriptorT) -> None:
        """✍️ Register a descriptor for future lookups."""
        ...

    def get(self, name: str) -> DescriptorT:
        """🔍 Retrieve a descriptor by name."""
        ...


class WorkloadQueue(Protocol[DescriptorT]):
    """🧱 Queue abstraction used prior to execution."""

    def enqueue(
        self,
        descriptor: DescriptorT,
        *,
        context: RuntimeContext,
    ) -> str:
        """🚀 Enqueue a descriptor and return a task identifier."""
        ...

    def dequeue(
        self,
        *,
        context: RuntimeContext,
    ) -> tuple[str, DescriptorT] | None:
        """🔄 Retrieve the next descriptor to process, if available."""
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
class BaseWorkloadDescriptor(WorkloadDescriptor):
    """🧾 Minimal descriptor implementation."""

    name: str
    kind: WorkloadKind
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BaseWorkloadResult(Generic[DescriptorT], WorkloadResult[DescriptorT]):
    """✅ Minimal result implementation with convenience helpers."""

    descriptor: DescriptorT
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
    "BaseWorkloadDescriptor",
    "BaseWorkloadResult",
    "WorkloadDescriptor",
    "WorkloadEventPayload",
    "WorkloadExecutor",
    "WorkloadKind",
    "WorkloadOutcome",
    "WorkloadQueue",
    "WorkloadRegistry",
    "WorkloadResult",
    "WorkloadSummaryReporter",
]
