from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Protocol

from ._model import TaskEntry, TaskSubmission


class TaskRegistryStore(Protocol):
    """💾 Storage abstraction backing the in-memory task registry."""

    def save(self, entry: TaskEntry) -> None:
        """🔄 Save a task entry to the registry.

        Args:
            entry: The task entry.
        """
        ...

    def load(self, name: str) -> TaskEntry:
        """🔍 Load a task entry from the registry.

        Args:
            name: The name of the task.
        """
        ...

    def remember_task_id(self, task_id: str, submission: TaskSubmission) -> None:
        """🔄 Associate a queue-provided task ID with a submission.

        Args:
            task_id: The task ID.
            submission: The task submission.
        """
        ...

    def load_by_task_id(self, task_id: str) -> TaskSubmission | None:
        """🔍 Load a task submission from the registry by task ID.

        Args:
            task_id: The task ID.
        """
        ...


@dataclass
class InMemoryTaskRegistryStore(TaskRegistryStore):
    """💾 Beginner-friendly store retaining task entries in RAM."""

    by_name: Dict[str, TaskEntry] = field(default_factory=dict)
    by_task_id: Dict[str, TaskSubmission] = field(default_factory=dict)

    def save(self, entry: TaskEntry) -> None:
        """🔄 Save a task entry to the registry.

        Args:
            entry: The task entry.
        """
        self.by_name[entry.name] = entry

    def load(self, name: str) -> TaskEntry:
        """🔍 Load a task entry from the registry.

        Args:
            name: The name of the task.
        """
        return self.by_name[name]

    def remember_task_id(self, task_id: str, submission: TaskSubmission) -> None:
        """🔄 Associate a queue-provided task ID with a submission.

        Args:
            task_id: The task ID.
            submission: The task submission.
        """
        self.by_task_id[task_id] = submission

    def load_by_task_id(self, task_id: str) -> TaskSubmission | None:
        """🔍 Load a task submission from the registry by task ID.

        Args:
            task_id: The task ID.
        """
        return self.by_task_id.get(task_id)


class TaskRegistry:
    """🗂️ Thread-safe registry delegating persistence to a store."""

    def __init__(self, *, store: TaskRegistryStore | None = None) -> None:
        self._store = store or InMemoryTaskRegistryStore()
        self._lock = RLock()

    def add(self, entry: TaskEntry) -> None:
        """🔄 Add a task entry to the registry.

        Args:
            entry: The task entry.
        """
        with self._lock:
            self._store.save(entry)

    def get(self, name: str) -> TaskEntry:
        """🔍 Get a task entry from the registry.

        Args:
            name: The name of the task.
        """
        with self._lock:
            return self._store.load(name)

    def remember_task_id(self, task_id: str, submission: TaskSubmission) -> None:
        """🔄 Associate a queue-provided task ID with a submission.

        Args:
            task_id: The task ID.
            submission: The task submission.
        """
        with self._lock:
            self._store.remember_task_id(task_id, submission)

    def get_by_task_id(self, task_id: str) -> TaskSubmission | None:
        """🔍 Get a task submission from the registry by task ID.

        Args:
            task_id: The task ID.
        """
        with self._lock:
            return self._store.load_by_task_id(task_id)


__all__ = ["InMemoryTaskRegistryStore", "TaskRegistry", "TaskRegistryStore"]
