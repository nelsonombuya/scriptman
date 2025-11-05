"""📦 Scheduler job models shared across the scheduler package."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time, tzinfo

from scriptman.powers.generics import Func
from scriptman.powers.scheduler.triggers import SchedulerTrigger


@dataclass(slots=True)
class Job:
    """🧱 Public job definition registered through the scheduler facade.

    Args:
        id: Unique identifier for the job.
        name: Human-friendly name used for logging and summaries.
        func: Callable that performs the job logic.
        trigger: Trigger controlling when the job should run.
        enabled: Whether the job is scheduled immediately.
        max_instances: Maximum concurrent executions allowed.
        start_time: Optional lower bound when using windowed triggers.
        end_time: Optional upper bound when using windowed triggers.
        time_zone: Timezone used for windowed scheduling.
    """

    id: str
    name: str
    func: Func[..., object]
    trigger: SchedulerTrigger
    enabled: bool = True
    max_instances: int = 1
    start_time: time | None = None
    end_time: time | None = None
    time_zone: tzinfo | None = None

    def __post_init__(self) -> None:
        if not self.id or self.id.isspace():
            raise ValueError("Job id cannot be empty")
        if not self.name or self.name.isspace():
            raise ValueError("Job name cannot be empty")
        if self.max_instances < 1:
            raise ValueError("max_instances must be at least 1")
        if self.start_time and self.end_time and self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")


__all__ = ["Job"]
