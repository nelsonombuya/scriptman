from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Iterable, Protocol

from ._facade_types import ServiceEntry, ServiceOptions


class ServiceRegistryStore(Protocol):
    """💾 Persistence interface for service entries and options."""

    def save(self, entry: ServiceEntry, options: ServiceOptions) -> None:
        """✍️ Save a service entry and options to the registry."""
        ...

    def load(self, name: str) -> tuple[ServiceEntry, ServiceOptions]:
        """🔍 Load a service entry and options from the registry."""
        ...

    def load_all(self) -> Iterable[tuple[ServiceEntry, ServiceOptions]]:
        """🔍 Load all services from the registry."""
        ...

    def delete(self, name: str) -> None:
        """🗑️ Delete a service from the registry."""
        ...


@dataclass
class InMemoryServiceRegistryStore(ServiceRegistryStore):
    """🧠 In-memory store ideal for tests and default usage."""

    _entries: Dict[str, ServiceEntry] = field(default_factory=dict)
    _options: Dict[str, ServiceOptions] = field(default_factory=dict)

    def save(self, entry: ServiceEntry, options: ServiceOptions) -> None:
        """✍️ Save a service entry and options to the registry."""
        self._entries[entry.name] = entry
        self._options[entry.name] = options

    def load(self, name: str) -> tuple[ServiceEntry, ServiceOptions]:
        """🔍 Load a service entry and options from the registry."""
        return self._entries[name], self._options[name]

    def load_all(self) -> Iterable[tuple[ServiceEntry, ServiceOptions]]:
        """🔍 Load all services from the registry."""
        for name, entry in self._entries.items():
            yield entry, self._options[name]

    def delete(self, name: str) -> None:
        """🗑️ Delete a service from the registry."""
        self._entries.pop(name, None)
        self._options.pop(name, None)


class ServiceRegistry:
    """🗂️ Thread-safe registry delegating persistence to a store."""

    def __init__(self, *, store: ServiceRegistryStore | None = None) -> None:
        self._store = store or InMemoryServiceRegistryStore()
        self._lock = RLock()

    def add(
        self,
        entry: ServiceEntry,
        options: ServiceOptions | None = None,
    ) -> None:
        """✍️ Add a service entry and options to the registry."""
        with self._lock:
            self._store.save(entry, options or ServiceOptions())

    def get(self, name: str) -> ServiceEntry:
        """🔍 Get a service entry from the registry."""
        with self._lock:
            entry, _ = self._store.load(name)
            return entry

    def get_options(self, name: str) -> ServiceOptions:
        with self._lock:
            _, options = self._store.load(name)
            return options

    def list(self) -> list[tuple[ServiceEntry, ServiceOptions]]:
        """🔍 Get the list of all services from the registry."""
        with self._lock:
            return list(self._store.load_all())

    def remove(self, name: str) -> None:
        """🗑️ Remove a service from the registry.

        Args:
            name: The name of the service.

        Raises:
            RuntimeError: If the service is not registered.
        """
        with self._lock:
            self._store.delete(name)


__all__ = [
    "InMemoryServiceRegistryStore",
    "ServiceRegistry",
    "ServiceRegistryStore",
]
