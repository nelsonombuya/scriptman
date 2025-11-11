"""🚀 Workload contracts shared by tasks, services, and schedules."""

from ._protocols import (
    BaseWorkloadDescriptor,
    BaseWorkloadResult,
    WorkloadDescriptor,
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
    "BaseWorkloadDescriptor",
    "BaseWorkloadResult",
    "WorkloadDescriptor",
    "WorkloadEventPayload",
    "WorkloadExecutor",
    "WorkloadKind",
    "WorkloadOutcome",
    "WorkloadQueue",
    "WorkloadResult",
    "WorkloadSummaryReporter",
    "WorkloadRegistry",
]
