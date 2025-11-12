from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Iterable, Protocol

from ._facade_types import ServiceDescriptor, ServiceOptions


class ServiceRegistryStore(Protocol):
    """💾 Persistence interface for service descriptors and options."""

    def save(self, descriptor: ServiceDescriptor, options: ServiceOptions) -> None:
        """✍️ Save a service descriptor and options to the registry.

        Args:
            descriptor: The service descriptor.
            options: The service options.

        Raises:
            RuntimeError: If the service is already registered.
        """
        ...

    def load(self, name: str) -> tuple[ServiceDescriptor, ServiceOptions]:
        """🔍 Load a service descriptor and options from the registry.

        Args:
            name: The name of the service.

        Returns:
            tuple[ServiceDescriptor, ServiceOptions]: The service descriptor and options.

        Raises:
            RuntimeError: If the service is not registered.
        """
        ...

    def load_all(self) -> Iterable[tuple[ServiceDescriptor, ServiceOptions]]:
        """🔍 Load all services from the registry.

        Returns:
            Iterable[tuple[ServiceDescriptor, ServiceOptions]]: The list of all services.

        Raises:
            RuntimeError: If the service is not registered.
        """
        ...

    def delete(self, name: str) -> None:
        """🗑️ Delete a service from the registry.

        Args:
            name: The name of the service.

        Raises:
            RuntimeError: If the service is not registered.
        """
        ...


@dataclass
class InMemoryServiceRegistryStore(ServiceRegistryStore):
    """🧠 In-memory store ideal for tests and default usage."""

    _descriptors: Dict[str, ServiceDescriptor] = field(default_factory=dict)
    _options: Dict[str, ServiceOptions] = field(default_factory=dict)

    def save(self, descriptor: ServiceDescriptor, options: ServiceOptions) -> None:
        """✍️ Save a service descriptor and options to the registry.

        Args:
            descriptor: The service descriptor.
            options: The service options.

        Raises:
            RuntimeError: If the service is already registered.
        """
        self._descriptors[descriptor.name] = descriptor
        self._options[descriptor.name] = options

    def load(self, name: str) -> tuple[ServiceDescriptor, ServiceOptions]:
        """🔍 Load a service descriptor and options from the registry.

        Args:
            name: The name of the service.

        Returns:
            tuple[ServiceDescriptor, ServiceOptions]: The service descriptor and options.

        Raises:
            RuntimeError: If the service is not registered.
        """
        return self._descriptors[name], self._options[name]

    def load_all(self) -> Iterable[tuple[ServiceDescriptor, ServiceOptions]]:
        """🔍 Load all services from the registry.

        Returns:
            Iterable[tuple[ServiceDescriptor, ServiceOptions]]: The list of all services.

        Raises:
            RuntimeError: If the service is not registered.
        """
        for name, descriptor in self._descriptors.items():
            yield descriptor, self._options[name]

    def delete(self, name: str) -> None:
        """🗑️ Delete a service from the registry.

        Args:
            name: The name of the service.

        Raises:
            RuntimeError: If the service is not registered.
        """
        self._descriptors.pop(name, None)
        self._options.pop(name, None)


class ServiceRegistry:
    """🗂️ Thread-safe registry delegating persistence to a store."""

    def __init__(self, *, store: ServiceRegistryStore | None = None) -> None:
        self._store = store or InMemoryServiceRegistryStore()
        self._lock = RLock()

    def add(
        self,
        descriptor: ServiceDescriptor,
        options: ServiceOptions | None = None,
    ) -> None:
        """✍️ Add a service descriptor and options to the registry.

        Args:
            descriptor: The service descriptor.
            options: The service options.

        Raises:
            RuntimeError: If the service is already registered.
        """
        with self._lock:
            self._store.save(descriptor, options or ServiceOptions())

    def get(self, name: str) -> ServiceDescriptor:
        """🔍 Get a service descriptor from the registry.

        Args:
            name: The name of the service.

        Returns:
            ServiceDescriptor: The service descriptor.

        Raises:
            RuntimeError: If the service is not registered.
        """
        with self._lock:
            descriptor, _ = self._store.load(name)
            return descriptor

    def get_options(self, name: str) -> ServiceOptions:
        """🔍 Get the options for a service from the registry.

        Args:
            name: The name of the service.

        Returns:
            ServiceOptions: The options for the service.

        Raises:
            RuntimeError: If the service is not registered.
        """
        with self._lock:
            _, options = self._store.load(name)
            return options

    def list(self) -> list[tuple[ServiceDescriptor, ServiceOptions]]:
        """🔍 Get the list of all services from the registry.

        Returns:
            list[tuple[ServiceDescriptor, ServiceOptions]]: The list of all services.

        Raises:
            RuntimeError: If the service is not registered.
        """
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
