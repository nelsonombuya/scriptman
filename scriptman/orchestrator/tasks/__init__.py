"""🚀 Tasks façade contracts for the Ouroboros Runtime Orchestrator."""

from ._facade import TaskDescriptor, TaskExecutionResult, TaskRegistry, TasksFacade
from ._registry import InMemoryTaskRegistryStore, TaskRegistryImpl, TaskRegistryStore

__all__ = [
    "InMemoryTaskRegistryStore",
    "TaskDescriptor",
    "TaskExecutionResult",
    "TaskRegistry",
    "TaskRegistryImpl",
    "TaskRegistryStore",
    "TasksFacade",
]
