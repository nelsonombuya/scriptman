"""🚀 Workload contracts shared by tasks, services, and schedules."""

from ._protocols import (
    BaseWorkloadEntry,
    BaseWorkloadResult,
    WorkloadEntry,
    WorkloadEventPayload,
    WorkloadExecutor,
    WorkloadKind,
    WorkloadOutcome,
    WorkloadQueue,
    WorkloadRegistry,
    WorkloadResult,
    WorkloadSummaryReporter,
)

__all__ = [
    "BaseWorkloadEntry",
    "BaseWorkloadResult",
    "WorkloadEntry",
    "WorkloadEventPayload",
    "WorkloadExecutor",
    "WorkloadKind",
    "WorkloadOutcome",
    "WorkloadQueue",
    "WorkloadResult",
    "WorkloadSummaryReporter",
    "WorkloadRegistry",
]
