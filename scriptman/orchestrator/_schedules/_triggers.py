from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Mapping, Protocol, cast

from typing_extensions import runtime_checkable


@runtime_checkable
class ScheduleTrigger(Protocol):
    """🔔 Contract for computing the next fire time."""

    def next_fire(self, after: datetime) -> datetime | None:
        """🕒 Compute the next fire time.

        Args:
            after: The time after which the trigger should fire.
        """
        ...

    def preview(
        self,
        count: int,
        *,
        start: datetime | None = None,
    ) -> list[datetime]:
        """🔍 Preview the next fire times.

        Args:
            count: The number of fire times to preview.
            start: The start time to preview from.
        """
        ...

    def serialize(self) -> Mapping[str, object]:
        """🔍 Serialize the trigger to a mapping.

        Returns:
            The serialized trigger.
        """
        ...

    def _initial_datetime(
        self, start: datetime | None, offset: datetime | None
    ) -> datetime:
        """🕒 Compute the initial datetime.

        Args:
            start: The start time.
            offset: The offset time.
        """
        if start is not None:
            return start
        if offset is not None:
            return offset
        return datetime.now(tz=timezone.utc)


@dataclass
class IntervalTrigger(ScheduleTrigger):
    """⏱️ Fire every fixed interval."""

    every: timedelta
    offset: datetime | None = None

    def next_fire(self, after: datetime) -> datetime | None:
        """🕒 Compute the next fire time.

        Args:
            after: The time after which the trigger should fire.
        """
        reference = self.offset or after
        delta = self.every
        if delta.total_seconds() <= 0:
            return None
        elapsed = max((after - reference).total_seconds(), 0)
        steps = int(elapsed // delta.total_seconds()) + 1
        return reference + delta * steps

    def preview(
        self,
        count: int,
        *,
        start: datetime | None = None,
    ) -> list[datetime]:
        """🔍 Preview the next fire times.

        Args:
            count: The number of fire times to preview.
            start: The start time to preview from.
        """
        times: list[datetime] = []
        current = self._initial_datetime(start, self.offset)
        for _ in range(count):
            next_time = self.next_fire(current)
            if next_time is None:
                break
            current = next_time
            times.append(current)
        return times

    def serialize(self) -> Mapping[str, object]:
        return {
            "type": "interval",
            "every": self.every.total_seconds(),
            "offset": self.offset.isoformat() if self.offset else None,
        }


@dataclass
class OneTimeTrigger(ScheduleTrigger):
    """🎯 Fire once at a specific moment."""

    at: datetime

    def next_fire(self, after: datetime) -> datetime | None:
        """🕒 Compute the next fire time.

        Args:
            after: The time after which the trigger should fire.
        """
        return self.at if after <= self.at else None

    def preview(
        self,
        count: int,
        *,
        start: datetime | None = None,
    ) -> list[datetime]:
        """🔍 Preview the next fire times.

        Args:
            count: The number of fire times to preview.
            start: The start time to preview from.
        """
        return [self.at] if (start or datetime.now(tz=timezone.utc)) <= self.at else []

    def serialize(self) -> Mapping[str, object]:
        """🔍 Serialize the trigger to a mapping.

        Returns:
            The serialized trigger.
        """
        return {"type": "one_time", "at": self.at.isoformat()}


@dataclass
class CronTrigger(ScheduleTrigger):
    """🕰️ Cron expression trigger."""

    expression: str
    timezone_id: str | None = None

    def next_fire(self, after: datetime) -> datetime | None:
        try:
            from croniter import croniter
        except ImportError as exc:  # pragma: no cover - optional dep
            raise ImportError(
                "Install croniter for cron support: pip install croniter"
            ) from exc
        tz_aware = after
        iterator = croniter(self.expression, tz_aware)
        return cast(datetime, iterator.get_next(datetime))

    def preview(
        self,
        count: int,
        *,
        start: datetime | None = None,
    ) -> list[datetime]:
        """🔍 Preview the next fire times.

        Args:
            count: The number of fire times to preview.
            start: The start time to preview from.
        """
        try:
            from croniter import croniter
        except ImportError as exc:  # pragma: no cover - optional dep
            raise ImportError(
                "Install croniter for cron support: pip install croniter"
            ) from exc
        tz_start = start or datetime.now(tz=timezone.utc)
        iterator = croniter(self.expression, tz_start)
        return [cast(datetime, iterator.get_next(datetime)) for _ in range(count)]

    def serialize(self) -> Mapping[str, object]:
        """🔍 Serialize the trigger to a mapping.

        Returns:
            The serialized trigger.
        """
        return {
            "type": "cron",
            "expression": self.expression,
            "timezone": self.timezone_id,
        }


@dataclass
class EventTrigger(ScheduleTrigger):
    """📡 Fire when a specific event is published."""

    topic: str
    predicate: Callable[[Mapping[str, object]], bool] | None = None

    def next_fire(self, after: datetime) -> datetime | None:
        """🕒 Compute the next fire time.

        Args:
            after: The time after which the trigger should fire.
        """
        return None  # determined by incoming events

    def preview(
        self,
        count: int,
        *,
        start: datetime | None = None,
    ) -> list[datetime]:
        """🔍 Preview the next fire times.

        Args:
            count: The number of fire times to preview.
            start: The start time to preview from.
        """
        return []

    def serialize(self) -> Mapping[str, object]:
        """🔍 Serialize the trigger to a mapping.

        Returns:
            The serialized trigger.
        """
        return {"type": "event", "topic": self.topic}


__all__ = [
    "CronTrigger",
    "EventTrigger",
    "IntervalTrigger",
    "OneTimeTrigger",
    "ScheduleTrigger",
]
