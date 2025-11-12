from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Iterable, Protocol

from ._types import ScheduleDescriptor, ScheduleOptions


class ScheduleRegistryStore(Protocol):
    def save(self, descriptor: ScheduleDescriptor, options: ScheduleOptions) -> None:
        """🔄 Save a schedule descriptor and options to the registry.

        Args:
            descriptor: The schedule descriptor.
            options: The schedule options.
        """
        ...

    def load(self, name: str) -> tuple[ScheduleDescriptor, ScheduleOptions]:
        """🔍 Load a schedule descriptor and options from the registry.

        Args:
            name: The name of the schedule.
        """
        ...

    def load_all(self) -> Iterable[tuple[ScheduleDescriptor, ScheduleOptions]]:
        """🔍 Load all schedules from the registry."""
        ...

    def delete(self, name: str) -> None:
        """🗑️ Delete a schedule from the registry.

        Args:
            name: The name of the schedule.
        """
        ...


@dataclass
class InMemoryScheduleRegistryStore(ScheduleRegistryStore):
    """💾 In-memory store for schedules."""

    descriptors: Dict[str, ScheduleDescriptor] = field(default_factory=dict)
    options: Dict[str, ScheduleOptions] = field(default_factory=dict)
    paused: Dict[str, bool] = field(default_factory=dict)

    def save(self, descriptor: ScheduleDescriptor, options: ScheduleOptions) -> None:
        """🔄 Save a schedule descriptor and options to the registry.

        Args:
            descriptor: The schedule descriptor.
            options: The schedule options.
        """
        self.descriptors[descriptor.name] = descriptor
        self.options[descriptor.name] = options

    def load(self, name: str) -> tuple[ScheduleDescriptor, ScheduleOptions]:
        """🔍 Load a schedule descriptor and options from the registry.

        Args:
            name: The name of the schedule.
        """
        return self.descriptors[name], self.options[name]

    def load_all(self) -> Iterable[tuple[ScheduleDescriptor, ScheduleOptions]]:
        """🔍 Load all schedules from the registry."""
        for name, descriptor in self.descriptors.items():
            yield descriptor, self.options[name]

    def delete(self, name: str) -> None:
        """🗑️ Delete a schedule from the registry.

        Args:
            name: The name of the schedule.
        """
        self.descriptors.pop(name, None)
        self.options.pop(name, None)
        self.paused.pop(name, None)


class ScheduleRegistry:
    """🗂️ Registry providing pause/resume semantics."""

    def __init__(self, *, store: ScheduleRegistryStore | None = None) -> None:
        self._store = store or InMemoryScheduleRegistryStore()
        self._lock = RLock()

    def add(
        self,
        descriptor: ScheduleDescriptor,
        options: ScheduleOptions | None = None,
    ) -> None:
        """🔄 Add a schedule descriptor and options to the registry.

        Args:
            descriptor: The schedule descriptor.
            options: The schedule options.
        """
        with self._lock:
            self._store.save(descriptor, options or ScheduleOptions())

    def get(self, name: str) -> ScheduleDescriptor:
        """🔍 Get a schedule descriptor from the registry.

        Args:
            name: The name of the schedule.
        """
        with self._lock:
            descriptor, _ = self._store.load(name)
            return descriptor

    def get_options(self, name: str) -> ScheduleOptions:
        """🔍 Get the options for a schedule from the registry.

        Args:
            name: The name of the schedule.
        """
        with self._lock:
            _, options = self._store.load(name)
            return options

    def list(self) -> list[tuple[ScheduleDescriptor, ScheduleOptions]]:
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
