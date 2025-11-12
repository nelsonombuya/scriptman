"""🚀 Tasks façade contracts for the Ouroboros Runtime Orchestrator."""

from scriptman.orchestrator._logging import (
    WorkloadLoggingOptions,
    configure_logging,
    extract_logging_options,
    log_to,
    workload_log_sink,
)

from ._executor import SynchronousTaskExecutor
from ._facade import TaskRegistry, TasksFacade
from ._model import (
    TaskConfigBinding,
    TaskEntry,
    TaskExecutionResult,
    TaskResourceSpec,
    TaskRetryPolicy,
    TaskSlaPolicy,
    TaskSubmission,
    default_task_logging,
)
from ._queue import InMemoryTaskQueue
from ._registry import InMemoryTaskRegistryStore, TaskRegistryStore
from ._reporter import NoOpTaskReporter

__all__ = [
    "InMemoryTaskRegistryStore",
    "InMemoryTaskQueue",
    "NoOpTaskReporter",
    "TaskConfigBinding",
    "TaskEntry",
    "TaskExecutionResult",
    "TaskResourceSpec",
    "TaskRetryPolicy",
    "TaskSlaPolicy",
    "TaskSubmission",
    "WorkloadLoggingOptions",
    "TaskRegistry",
    "TaskRegistryStore",
    "TasksFacade",
    "SynchronousTaskExecutor",
    "default_task_logging",
    "configure_logging",
    "extract_logging_options",
    "log_to",
    "workload_log_sink",
]
