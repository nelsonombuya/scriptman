from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generic, Literal, Mapping, MutableMapping, Sequence

from scriptman.orchestrator._generics import P, R, TypedAnyFunc
from scriptman.orchestrator._logging import WorkloadLoggingOptions
from scriptman.orchestrator._workloads import (
    WorkloadEntry,
    WorkloadKind,
    WorkloadOutcome,
    WorkloadResult,
)

ResourceClass = Literal["light", "standard", "heavy"]
RetryStrategy = Literal["none", "fixed", "linear", "exponential"]


def default_task_logging() -> WorkloadLoggingOptions:
    """🔍 Default logging options for tasks."""
    return WorkloadLoggingOptions(
        path_template="logs/tasks/{workload}/{date}/{run_id}.log",
    )


@dataclass(slots=True)
class TaskRetryPolicy:
    """🔁 Retry configuration for task executions."""

    max_attempts: int = 0
    interval_seconds: float = 0.0
    strategy: RetryStrategy = "none"
    jitter_seconds: float | None = None
    max_interval_seconds: float | None = None


@dataclass(slots=True)
class TaskSlaPolicy:
    """⏱️ SLA expectations for a workload run."""

    deadline_seconds: float | None = None
    cancel_on_timeout: bool = False


@dataclass(slots=True)
class TaskResourceSpec:
    """📦 Resource intents declared by a task."""

    queue: str | None = None
    priority: int = 0
    cpu_class: ResourceClass = "standard"
    io_class: ResourceClass = "standard"
    memory_class: ResourceClass = "standard"
    tags: tuple[str, ...] = ()


@dataclass(slots=True)
class TaskConfigBinding:
    """⚙️ Configuration lineage requirements for execution."""

    min_generation: str | None = None
    max_generation: str | None = None
    required_flags: tuple[str, ...] = ()


@dataclass(slots=True)
class TaskEntry(Generic[P, R], WorkloadEntry):
    """🧾 Immutable description of a registered task.

    Captures the task definition with all its metadata, policies, and the callable
    target. Uses generics to preserve type information through the orchestration
    stack, enabling full type inference for power users while remaining simple
    for beginners.

    The target uses UntypedCallable (loosely typed) for maximum flexibility,
    allowing any callable type (sync, async, generators) to be registered
    without type constraints. This trades type information for flexibility.
    """

    name: str
    target: TypedAnyFunc[P, R]
    labels: Sequence[str] = field(default_factory=tuple)
    sla: TaskSlaPolicy = field(default_factory=TaskSlaPolicy)
    metadata: Mapping[str, object] = field(default_factory=dict)
    retry: TaskRetryPolicy = field(default_factory=TaskRetryPolicy)
    config: TaskConfigBinding = field(default_factory=TaskConfigBinding)
    resources: TaskResourceSpec = field(default_factory=TaskResourceSpec)
    logging: WorkloadLoggingOptions = field(default_factory=default_task_logging)

    @property
    def kind(self) -> WorkloadKind:
        """🔍 Workload kind identifier for Command Deck events."""
        return "task"


@dataclass(slots=True)
class TaskSubmission(Generic[P, R], WorkloadEntry):
    """📦 Concrete invocation of a task entry.

    Represents a specific execution request with arguments, metadata, and
    correlation tracking. Carries the entry definition alongside runtime-specific
    data like task IDs and retry attempts.

    Uses generics to maintain type relationships with the entry and execution
    results, enabling end-to-end type safety through the orchestration pipeline.
    """

    entry: TaskEntry[P, R]
    task_id: str | None = None
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    extra_metadata: Mapping[str, object] = field(default_factory=dict)
    correlation_id: str | None = None
    attempt: int = 1

    @property
    def name(self) -> str:
        return self.entry.name

    @property
    def kind(self) -> WorkloadKind:
        return self.entry.kind

    @property
    def metadata(self) -> Mapping[str, object]:
        if not self.extra_metadata:
            return self.entry.metadata
        combined: dict[str, object] = dict(self.entry.metadata)
        combined.update(self.extra_metadata)
        return combined


@dataclass(slots=True)
class TaskExecutionResult(Generic[P, R], WorkloadResult[TaskEntry[P, R]]):
    """✅ Summary bundle returned after executing a task.

    Contains the complete execution report including timing, outcome, errors,
    and configuration lineage. Links back to the submission and entry through
    generics to maintain type relationships across the orchestration pipeline.

    Provides convenience properties like duration and run_id for easy access
    to common execution metrics.
    """

    task_id: str
    submission: TaskSubmission[P, R]
    outcome: WorkloadOutcome
    started_at: datetime
    finished_at: datetime
    config_generation: str | None = None
    detail: MutableMapping[str, Any] = field(default_factory=dict)
    result: object | None = None
    error: BaseException | None = None

    @property
    def duration(self) -> float:
        """⏱️ Duration in seconds between start and finish."""
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def run_id(self) -> str:
        """🔍 Alias for the workload run identifier."""
        return self.task_id

    @property
    def entry(self) -> TaskEntry[P, R]:
        """🔍 Entry that produced this execution.

        Returns:
            The task entry that was executed, maintaining generic type
            information for type-safe access to entry properties.
        """
        return self.submission.entry


__all__ = [
    "TaskConfigBinding",
    "TaskEntry",
    "TaskExecutionResult",
    "TaskResourceSpec",
    "TaskRetryPolicy",
    "TaskSlaPolicy",
    "default_task_logging",
    "TaskSubmission",
]
