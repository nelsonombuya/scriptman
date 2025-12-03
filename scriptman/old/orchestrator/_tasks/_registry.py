from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Generic

from scriptman.orchestrator._generics import P, R

from ._model import TaskEntry, TaskSubmission


class TaskRegistryStore(Generic[P, R], ABC):
    """💾 Storage abstraction backing the in-memory task registry.

    Defines the contract for persistent storage of task entries and submissions.
    Supports both name-based lookups for entries and task ID lookups for submissions.
    Uses generics to maintain type relationships with specific entry and submission types.

    Runtime enforcement ensures all stores implement save, load, remember_task_id,
    and load_by_task_id methods, making storage behavior predictable across
    implementations (in-memory, disk, Redis, etc.).
    """

    @abstractmethod
    def save(self, entry: TaskEntry[P, R]) -> None:
        """🔄 Save a task entry to the registry.

        Args:
            entry: The task entry to store. Must have a unique name
                within the registry.

        Raises:
            ValueError: If an entry with the same name already exists,
                unless the store supports overwriting.
        """
        ...

    @abstractmethod
    def load(self, name: str) -> TaskEntry[P, R]:
        """🔍 Load a task entry from the registry.

        Args:
            name: The unique name of the task entry.

        Returns:
            The task entry with the given name.

        Raises:
            KeyError: If no entry with the given name exists.
        """
        ...

    @abstractmethod
    def remember_task_id(self, task_id: str, submission: TaskSubmission[P, R]) -> None:
        """🔄 Associate a queue-provided task ID with a submission.

        Args:
            task_id: The task ID returned by the queue.
            submission: The task submission to associate with the ID.

        Stores the submission for later retrieval by task ID. Used to track
        queued tasks and their arguments across queue operations.
        """
        ...

    @abstractmethod
    def load_by_task_id(self, task_id: str) -> TaskSubmission[P, R] | None:
        """🔍 Load a task submission from the registry by task ID.

        Args:
            task_id: The task ID returned by the queue.

        Returns:
            The task submission associated with the ID, or None if not found.

        Used to retrieve submissions that were queued but not yet executed.
        """
        ...


@dataclass
class InMemoryTaskRegistryStore(Generic[P, R], TaskRegistryStore[P, R]):
    """💾 Beginner-friendly store retaining task entries in RAM."""

    by_name: Dict[str, TaskEntry[P, R]] = field(default_factory=dict)
    by_task_id: Dict[str, TaskSubmission[P, R]] = field(default_factory=dict)

    def save(self, entry: TaskEntry[P, R]) -> None:
        """🔄 Save a task entry to the registry.

        Args:
            entry: The task entry.
        """
        self.by_name[entry.name] = entry

    def load(self, name: str) -> TaskEntry[P, R]:
        """🔍 Load a task entry from the registry.

        Args:
            name: The name of the task.
        """
        return self.by_name[name]

    def remember_task_id(self, task_id: str, submission: TaskSubmission[P, R]) -> None:
        """🔄 Associate a queue-provided task ID with a submission.

        Args:
            task_id: The task ID.
            submission: The task submission.
        """
        self.by_task_id[task_id] = submission

    def load_by_task_id(self, task_id: str) -> TaskSubmission[P, R] | None:
        """🔍 Load a task submission from the registry by task ID.

        Args:
            task_id: The task ID.
        """
        return self.by_task_id.get(task_id)


class TaskRegistry(Generic[P, R]):
    """🗂️ Thread-safe registry delegating persistence to a store."""

    def __init__(self, *, store: TaskRegistryStore[P, R] | None = None) -> None:
        self._store = store or InMemoryTaskRegistryStore[P, R]()
        self._lock = RLock()

    def add(self, entry: TaskEntry[P, R]) -> None:
        """🔄 Add a task entry to the registry.

        Args:
            entry: The task entry.
        """
        with self._lock:
            self._store.save(entry)

    def get(self, name: str) -> TaskEntry[P, R]:
        """🔍 Get a task entry from the registry.

        Args:
            name: The name of the task.
        """
        with self._lock:
            return self._store.load(name)

    def remember_task_id(self, task_id: str, submission: TaskSubmission[P, R]) -> None:
        """🔄 Associate a queue-provided task ID with a submission.

        Args:
            task_id: The task ID.
            submission: The task submission.
        """
        with self._lock:
            self._store.remember_task_id(task_id, submission)

    def get_by_task_id(self, task_id: str) -> TaskSubmission[P, R] | None:
        """🔍 Get a task submission from the registry by task ID.

        Args:
            task_id: The task ID.
        """
        with self._lock:
            return self._store.load_by_task_id(task_id)


__all__ = ["InMemoryTaskRegistryStore", "TaskRegistry", "TaskRegistryStore"]
