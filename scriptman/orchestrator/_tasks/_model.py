from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping, MutableMapping, Sequence

from scriptman.orchestrator._generics import AnyCallable
from scriptman.orchestrator._logging import WorkloadLoggingOptions
from scriptman.orchestrator._workloads import WorkloadKind, WorkloadOutcome

TaskCallable = AnyCallable
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
    strategy: RetryStrategy = "none"
    interval_seconds: float = 0.0
    max_interval_seconds: float | None = None
    jitter_seconds: float | None = None


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
class TaskEntry:
    """🧾 Immutable description of a registered task."""

    name: str
    target: TaskCallable
    metadata: Mapping[str, object] = field(default_factory=dict)
    logging: WorkloadLoggingOptions = field(default_factory=default_task_logging)
    retry: TaskRetryPolicy = field(default_factory=TaskRetryPolicy)
    sla: TaskSlaPolicy = field(default_factory=TaskSlaPolicy)
    resources: TaskResourceSpec = field(default_factory=TaskResourceSpec)
    config: TaskConfigBinding = field(default_factory=TaskConfigBinding)
    labels: Sequence[str] = field(default_factory=tuple)

    @property
    def kind(self) -> WorkloadKind:
        """🔍 Workload kind identifier for Command Deck events."""
        return "task"


@dataclass(slots=True)
class TaskSubmission:
    """📦 Concrete invocation of a task entry."""

    entry: TaskEntry
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    extra_metadata: Mapping[str, object] = field(default_factory=dict)
    correlation_id: str | None = None
    attempt: int = 1
    task_id: str | None = None

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
class TaskExecutionResult:
    """✅ Summary bundle returned after executing a task."""

    task_id: str
    submission: TaskSubmission
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
    def entry(self) -> TaskEntry:
        """🔍 Entry that produced this execution."""
        return self.submission.entry


__all__ = [
    "TaskCallable",
    "TaskConfigBinding",
    "TaskEntry",
    "TaskExecutionResult",
    "TaskResourceSpec",
    "TaskRetryPolicy",
    "TaskSlaPolicy",
    "default_task_logging",
    "TaskSubmission",
]
