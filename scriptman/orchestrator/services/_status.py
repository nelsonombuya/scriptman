from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping

from scriptman.orchestrator.workloads import WorkloadOutcome


@dataclass(slots=True)
class ServiceStatus:
    """📊 Snapshot of a running service."""

    name: str
    state: WorkloadOutcome
    uptime_seconds: float
    restart_count: int
    last_heartbeat: datetime | None = None
    detail: Mapping[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ServiceOverview:
    """📋 Lightweight view for list outputs."""

    name: str
    state: WorkloadOutcome
    autostart: bool
    restart_policy: str
    metadata: Mapping[str, object] = field(default_factory=dict)


__all__ = ["ServiceStatus", "ServiceOverview"]
