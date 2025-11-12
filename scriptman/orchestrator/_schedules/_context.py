from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, MutableMapping

from scriptman.orchestrator._context import RuntimeContext


@dataclass
class ScheduleContext:
    """🛰️ Execution context passed to scheduled callables."""

    name: str
    metadata: Mapping[str, object]
    planned_time: datetime
    actual_time: datetime
    runtime_context: RuntimeContext
    detail: MutableMapping[str, object] = field(default_factory=dict)

    def record(self, **info: object) -> None:
        """🔔 Record additional information about the schedule execution.

        Args:
            **info: The additional information to record.
        """
        self.detail.update(info)
