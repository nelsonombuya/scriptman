from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Iterable, Protocol

from ._types import ScheduleEntry, ScheduleOptions


class ScheduleRegistryStore(Protocol):
    def save(self, entry: ScheduleEntry, options: ScheduleOptions) -> None:
        """🔄 Save a schedule entry and options to the registry."""
        ...

    def load(self, name: str) -> tuple[ScheduleEntry, ScheduleOptions]:
        """🔍 Load a schedule entry and options from the registry."""
        ...

    def load_all(self) -> Iterable[tuple[ScheduleEntry, ScheduleOptions]]:
        """🔍 Load all schedules from the registry."""
        ...

    def delete(self, name: str) -> None:
        """🗑️ Delete a schedule from the registry."""
        ...


@dataclass
class InMemoryScheduleRegistryStore(ScheduleRegistryStore):
    """💾 In-memory store for schedules."""

    entries: Dict[str, ScheduleEntry] = field(default_factory=dict)
    options: Dict[str, ScheduleOptions] = field(default_factory=dict)
    paused: Dict[str, bool] = field(default_factory=dict)

    def save(self, entry: ScheduleEntry, options: ScheduleOptions) -> None:
        """🔄 Save a schedule entry and options to the registry."""
        self.entries[entry.name] = entry
        self.options[entry.name] = options

    def load(self, name: str) -> tuple[ScheduleEntry, ScheduleOptions]:
        """🔍 Load a schedule entry and options from the registry."""
        return self.entries[name], self.options[name]

    def load_all(self) -> Iterable[tuple[ScheduleEntry, ScheduleOptions]]:
        """🔍 Load all schedules from the registry."""
        for name, entry in self.entries.items():
            yield entry, self.options[name]

    def delete(self, name: str) -> None:
        """🗑️ Delete a schedule from the registry."""
        self.entries.pop(name, None)
        self.options.pop(name, None)
        self.paused.pop(name, None)


class ScheduleRegistry:
    """🗂️ Registry providing pause/resume semantics."""

    def __init__(self, *, store: ScheduleRegistryStore | None = None) -> None:
        self._store = store or InMemoryScheduleRegistryStore()
        self._lock = RLock()

    def add(
        self,
        entry: ScheduleEntry,
        options: ScheduleOptions | None = None,
    ) -> None:
        """🔄 Add a schedule entry and options to the registry."""
        with self._lock:
            self._store.save(entry, options or ScheduleOptions())

    def get(self, name: str) -> ScheduleEntry:
        """🔍 Get a schedule entry from the registry."""
        with self._lock:
            entry, _ = self._store.load(name)
            return entry

    def get_options(self, name: str) -> ScheduleOptions:
        with self._lock:
            _, options = self._store.load(name)
            return options

    def list(self) -> list[tuple[ScheduleEntry, ScheduleOptions]]:
        """🔍 Get the list of all schedules from the registry."""
        with self._lock:
            return list(self._store.load_all())

    def remove(self, name: str) -> None:
        """🗑️ Remove a schedule from the registry.

        Args:
            name: The name of the schedule.
        """
        with self._lock:
            self._store.delete(name)


__all__ = [
    "InMemoryScheduleRegistryStore",
    "ScheduleRegistry",
    "ScheduleRegistryStore",
]
