"""⚡ Scheduler package exposing TaskManager-integrated scheduling tools."""

from __future__ import annotations

from typing import Any

from .api import TaskScheduler
from .models import Job
from .service import ScheduledJob, SchedulerService
from .triggers import (
    IntervalTrigger,
    OneTimeTrigger,
    SchedulerTrigger,
    TimeOfDayTrigger,
)


class _SchedulerProxy:
    """🪄 Lightweight proxy that delegates to ``TaskManager().scheduler``."""

    def _resolve(self) -> TaskScheduler:
        from scriptman.powers.tasks import TaskManager

        return TaskManager().scheduler

    def __getattr__(self, item: str) -> Any:  # pragma: no cover - simple delegation
        return getattr(self._resolve(), item)

    def __call__(self) -> TaskScheduler:
        return self._resolve()


scheduler = _SchedulerProxy()

__all__ = [
    "Job",
    "TaskScheduler",
    "SchedulerService",
    "ScheduledJob",
    "SchedulerTrigger",
    "IntervalTrigger",
    "TimeOfDayTrigger",
    "OneTimeTrigger",
    "scheduler",
]
