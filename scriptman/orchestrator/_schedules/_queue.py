from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass
from datetime import datetime
from typing import MutableMapping

from ._types import ScheduleDescriptor


@dataclass(order=True)
class QueueEntry:
    fire_time: datetime
    sequence: int
    name: str
    descriptor: ScheduleDescriptor


class ScheduleQueue:
    """📬 Priority queue for upcoming schedules."""

    def __init__(self) -> None:
        self._heap: list[QueueEntry] = []
        self._counter = itertools.count()
        self._index: MutableMapping[str, QueueEntry] = {}

    def enqueue(
        self,
        descriptor: ScheduleDescriptor,
        fire_time: datetime,
    ) -> str:
        """🔄 Enqueue a schedule.

        Args:
            descriptor: The schedule descriptor.
            fire_time: The fire time of the schedule.
        """
        entry = QueueEntry(
            fire_time=fire_time,
            name=descriptor.name,
            descriptor=descriptor,
            sequence=next(self._counter),
        )
        heapq.heappush(self._heap, entry)
        self._index[descriptor.name] = entry
        return descriptor.name

    def dequeue_due(self, now: datetime) -> QueueEntry | None:
        """🔄 Dequeue a schedule due.

        Args:
            now: The current time.
        """
        while self._heap:
            entry = self._heap[0]
            if entry.fire_time > now:
                return None
            heapq.heappop(self._heap)
            if self._index.pop(entry.name, None) is entry:
                return entry
        return None

    def peek_due(self, now: datetime) -> QueueEntry | None:
        """🔍 Peek a schedule due.

        Args:
            now: The current time.
        """
        if not self._heap:
            return None
        entry = self._heap[0]
        return entry if entry.fire_time <= now else None

    def next_entry(self) -> QueueEntry | None:
        """🔍 Get the next schedule entry."""
        while self._heap:
            entry = self._heap[0]
            if self._index.get(entry.name) is entry:
                return entry
            heapq.heappop(self._heap)
        return None

    def reschedule(
        self, descriptor: ScheduleDescriptor, fire_time: datetime | None
    ) -> None:
        """🔄 Reschedule a schedule.

        Args:
            descriptor: The schedule descriptor.
            fire_time: The fire time of the schedule.
        """
        self._index.pop(descriptor.name, None)
        if fire_time is not None:
            self.enqueue(descriptor, fire_time)

    def remove(self, name: str) -> None:
        """🔄 Remove a schedule.

        Args:
            name: The name of the schedule.
        """
        entry = self._index.pop(name, None)
        if not entry:
            return
        # lazy removal; heap entries will be skipped in dequeue_due

    def __len__(self) -> int:
        return len(self._heap)


__all__ = ["QueueEntry", "ScheduleQueue"]
