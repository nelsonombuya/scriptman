"""⏱️ Scheduler triggers defining when jobs should run."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, tzinfo
from typing import Optional


class SchedulerTrigger:
    """🧭 Base trigger interface used by the scheduler service."""

    def next_run(self, previous: Optional[datetime]) -> datetime:
        """🕒 Compute the next run timestamp.

        Args:
            previous: Timestamp of the last execution, or ``None`` if the job
                has never run before.

        Returns:
            The :class:`datetime` when the job should execute next.
        """

        raise NotImplementedError("Subclasses must implement next_run()")


@dataclass(slots=True)
class IntervalTrigger(SchedulerTrigger):
    """♻️ Trigger jobs on a fixed interval."""

    interval: timedelta

    def __post_init__(self) -> None:
        if self.interval.total_seconds() <= 0:
            raise ValueError("Interval must be greater than zero seconds")

    def next_run(self, previous: Optional[datetime]) -> datetime:
        """🕑 Calculate the next execution time.

        Args:
            previous: Most recent execution, or ``None`` for first run.

        Returns:
            Timestamp ``interval`` seconds after ``previous`` (or ``now`` when
            no prior execution exists).
        """

        base = previous or datetime.now()
        return base + self.interval


@dataclass(slots=True)
class TimeOfDayTrigger(SchedulerTrigger):
    """🌞 Trigger jobs once per day at a specific time of day."""

    at: time
    timezone: tzinfo | None = None

    def next_run(self, previous: Optional[datetime]) -> datetime:
        """🌅 Determine the next daily run.

        Args:
            previous: Previous execution time, ignored for this trigger.

        Returns:
            Next occurrence of ``at`` considering ``timezone``.
        """

        now = datetime.now(tz=self.timezone)
        candidate = now.replace(
            hour=self.at.hour,
            minute=self.at.minute,
            second=self.at.second,
            microsecond=self.at.microsecond,
        )
        if candidate <= now:
            candidate += timedelta(days=1)
        return candidate


@dataclass(slots=True)
class OneTimeTrigger(SchedulerTrigger):
    """🎯 Trigger a job exactly once at a specific :class:`datetime`."""

    run_at: datetime

    def next_run(self, previous: Optional[datetime]) -> datetime:
        """🎯 Return the single execution timestamp.

        Args:
            previous: Previous execution time or ``None`` if first run.

        Returns:
            ``run_at`` on first call, otherwise :data:`datetime.max` to prevent
            repeated scheduling.
        """

        if previous is None:
            return self.run_at
        return datetime.max


__all__ = ["SchedulerTrigger", "IntervalTrigger", "TimeOfDayTrigger", "OneTimeTrigger"]
