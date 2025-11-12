"""🚀 Tasks façade contracts for the Ouroboros Runtime Orchestrator."""

from scriptman.orchestrator._logging import (
    WorkloadLoggingOptions,
    configure_logging,
    extract_logging_options,
    log_to,
    workload_log_sink,
)

from ._facade import TaskDescriptor, TaskExecutionResult, TaskRegistry, TasksFacade
from ._registry import InMemoryTaskRegistryStore, TaskRegistryStore

__all__ = [
    "InMemoryTaskRegistryStore",
    "TaskDescriptor",
    "TaskExecutionResult",
    "WorkloadLoggingOptions",
    "TaskRegistry",
    "TaskRegistryStore",
    "TasksFacade",
    "configure_logging",
    "extract_logging_options",
    "log_to",
    "workload_log_sink",
]
