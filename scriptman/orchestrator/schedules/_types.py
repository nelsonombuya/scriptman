from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Mapping

from scriptman.orchestrator._logging import WorkloadLoggingOptions
from scriptman.orchestrator.workloads import WorkloadKind, WorkloadOutcome

if TYPE_CHECKING:
    from ._context import ScheduleContext
    from ._triggers import ScheduleTrigger

SchedulableCallable = Callable[["ScheduleContext"], Awaitable[Any] | Any]


@dataclass
class ScheduleOptions:
    """⚙️ Configure scheduler behaviour for a job."""

    misfire_policy: str = "run_immediately"
    timezone: str | None = None
    max_overlaps: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


def _default_schedule_logging() -> WorkloadLoggingOptions:
    return WorkloadLoggingOptions(
        path_template="logs/schedules/{workload}/{date}/{run_id}.log"
    )


@dataclass
class ScheduleDescriptor:
    """🧾 Immutable description of a scheduled job."""

    name: str
    target: SchedulableCallable
    trigger: "ScheduleTrigger"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    logging: WorkloadLoggingOptions = field(default_factory=_default_schedule_logging)

    @property
    def kind(self) -> WorkloadKind:
        return "schedule"


@dataclass
class ScheduleExecutionResult:
    """✅ Result emitted after a scheduled job fires."""

    descriptor: ScheduleDescriptor
    outcome: WorkloadOutcome
    planned_time: datetime
    actual_time: datetime
    detail: Mapping[str, Any] = field(default_factory=dict)
    error: BaseException | None = None

    @property
    def duration_seconds(self) -> float:
        return (self.actual_time - self.planned_time).total_seconds()


__all__ = [
    "SchedulableCallable",
    "ScheduleDescriptor",
    "ScheduleExecutionResult",
    "ScheduleOptions",
]
