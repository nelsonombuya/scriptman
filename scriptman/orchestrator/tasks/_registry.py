from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Protocol

from ._facade import TaskDescriptor


class TaskRegistryStore(Protocol):
    """💾 Storage abstraction backing the in-memory task registry."""

    def save(self, descriptor: TaskDescriptor) -> None: ...

    def load(self, name: str) -> TaskDescriptor: ...

    def load_by_task_id(self, task_id: str) -> TaskDescriptor | None: ...


@dataclass
class InMemoryTaskRegistryStore(TaskRegistryStore):
    """💾 Beginner-friendly store retaining task descriptors in RAM."""

    by_name: Dict[str, TaskDescriptor] = field(default_factory=dict)
    by_task_id: Dict[str, TaskDescriptor] = field(default_factory=dict)

    def save(self, descriptor: TaskDescriptor) -> None:
        self.by_name[descriptor.name] = descriptor

    def load(self, name: str) -> TaskDescriptor:
        return self.by_name[name]

    def load_by_task_id(self, task_id: str) -> TaskDescriptor | None:
        return self.by_task_id.get(task_id)


class TaskRegistryImpl:
    """🗂️ Thread-safe registry delegating persistence to a store."""

    def __init__(self, *, store: TaskRegistryStore | None = None) -> None:
        self._store = store or InMemoryTaskRegistryStore()
        self._lock = RLock()

    def add(self, descriptor: TaskDescriptor) -> None:
        with self._lock:
            self._store.save(descriptor)

    def get(self, name: str) -> TaskDescriptor:
        with self._lock:
            return self._store.load(name)

    def get_by_task_id(self, task_id: str) -> TaskDescriptor | None:
        with self._lock:
            return self._store.load_by_task_id(task_id)


__all__ = ["InMemoryTaskRegistryStore", "TaskRegistryImpl", "TaskRegistryStore"]


