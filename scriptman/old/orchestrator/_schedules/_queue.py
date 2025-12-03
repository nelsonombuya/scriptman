from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass
from datetime import datetime
from typing import MutableMapping

from ._types import ScheduleEntry


@dataclass(order=True)
class QueueEntry:
    fire_time: datetime
    sequence: int
    name: str
    entry: ScheduleEntry


class ScheduleQueue:
    """📬 Priority queue for upcoming schedules."""

    def __init__(self) -> None:
        self._heap: list[QueueEntry] = []
        self._counter = itertools.count()
        self._index: MutableMapping[str, QueueEntry] = {}

    def enqueue(
        self,
        entry: ScheduleEntry,
        fire_time: datetime,
    ) -> str:
        """🔄 Enqueue a schedule.

        Args:
            entry: The schedule entry.
            fire_time: The fire time of the schedule.
        """
        queue_entry = QueueEntry(
            fire_time=fire_time,
            name=entry.name,
            entry=entry,
            sequence=next(self._counter),
        )
        heapq.heappush(self._heap, queue_entry)
        self._index[entry.name] = queue_entry
        return entry.name

    def dequeue_due(self, now: datetime) -> QueueEntry | None:
        """🔄 Dequeue a schedule due.

        Args:
            now: The current time.
        """
        while self._heap:
            queue_entry = self._heap[0]
            if queue_entry.fire_time > now:
                return None
            heapq.heappop(self._heap)
            if self._index.pop(queue_entry.name, None) is queue_entry:
                return queue_entry
        return None

    def peek_due(self, now: datetime) -> QueueEntry | None:
        """🔍 Peek a schedule due.

        Args:
            now: The current time.
        """
        if not self._heap:
            return None
        queue_entry = self._heap[0]
        return queue_entry if queue_entry.fire_time <= now else None

    def next_entry(self) -> QueueEntry | None:
        """🔍 Get the next schedule entry."""
        while self._heap:
            queue_entry = self._heap[0]
            if self._index.get(queue_entry.name) is queue_entry:
                return queue_entry
            heapq.heappop(self._heap)
        return None

    def reschedule(self, entry: ScheduleEntry, fire_time: datetime | None) -> None:
        """🔄 Reschedule a schedule.

        Args:
            entry: The schedule entry.
            fire_time: The fire time of the schedule.
        """
        self._index.pop(entry.name, None)
        if fire_time is not None:
            self.enqueue(entry, fire_time)

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
