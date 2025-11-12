from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, TypeVar

from scriptman.orchestrator._generics import ContextFunc
from scriptman.orchestrator._logging import WorkloadLoggingOptions
from scriptman.orchestrator._workloads import WorkloadKind, WorkloadOutcome

from ._context import ServiceContext
from ._policies import RestartPolicy

ServiceReturn = TypeVar("ServiceReturn")
ServiceCallable = ContextFunc[ServiceContext, ServiceReturn]


@dataclass
class ServiceOptions:
    """⚙️ Configure service runtime behaviour."""

    autostart: bool = True
    daemon: bool = True
    graceful_timeout: float = 30.0
    heartbeat_interval: float | None = None
    restart_policy: RestartPolicy = field(default_factory=RestartPolicy.always)
    metadata: Mapping[str, Any] = field(default_factory=dict)


def default_service_logging() -> WorkloadLoggingOptions:
    """🔍 Default logging options for services."""
    return WorkloadLoggingOptions(
        path_template="logs/services/{workload}/{date}/{run_id}.log"
    )


@dataclass
class ServiceDescriptor:
    """🧾 Immutable description of a registered service."""

    name: str
    target: ServiceCallable[Any]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    logging: WorkloadLoggingOptions = field(default_factory=default_service_logging)

    @property
    def kind(self) -> WorkloadKind:
        return "service"


@dataclass
class ServiceExecutionResult:
    """✅ Execution report emitted by the supervisor."""

    descriptor: ServiceDescriptor
    outcome: WorkloadOutcome
    started_at: datetime
    finished_at: datetime
    detail: Mapping[str, Any] = field(default_factory=dict)
    error: BaseException | None = None

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()


__all__ = [
    "ServiceCallable",
    "ServiceDescriptor",
    "ServiceExecutionResult",
    "ServiceOptions",
]
