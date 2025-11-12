from __future__ import annotations

from typing import Any, Mapping

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._events import RuntimeEventTopic, make_event
from scriptman.orchestrator._logging import extract_logging_options
from scriptman.orchestrator.services._registry import ServiceRegistry
from scriptman.orchestrator.services._status import ServiceOverview, ServiceStatus

from ._facade_types import (
    ServiceCallable,
    ServiceDescriptor,
    ServiceOptions,
    default_service_logging,
)
from ._supervisor import ServiceSupervisor


class ServicesFacade:
    """🚀 Manage long-running services with restart supervision."""

    def __init__(
        self,
        *,
        context: RuntimeContext,
        registry: ServiceRegistry | None = None,
        supervisor: ServiceSupervisor | None = None,
    ) -> None:
        self._ctx = context
        self._registry = registry or ServiceRegistry()
        self._supervisor = supervisor or ServiceSupervisor(context)

    def register(
        self,
        name: str,
        target: ServiceCallable[Any],
        *,
        options: ServiceOptions | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        """✍️ Register a new service with the underlying registry.

        Args:
            name: The name of the service.
            target: The target service callable.
            options: The options for the service.
            metadata: The metadata for the service.

        Raises:
            RuntimeError: If the service is already registered.
        """
        descriptor = ServiceDescriptor(
            name=name,
            target=target,
            metadata=metadata or {},
        )
        opts = options or ServiceOptions()
        default_logging = default_service_logging()
        decorator_logging = extract_logging_options(target)
        if descriptor.logging == default_logging and decorator_logging != default_logging:
            descriptor.logging = decorator_logging
        self._registry.add(descriptor, opts)
        register_event = make_event(
            RuntimeEventTopic.SERVICE_REGISTERED,
            timestamp_factory=self._ctx.timestamp,
            payload={
                "workload_kind": "service",
                "workload_name": name,
                "metadata": descriptor.metadata,
            },
        )
        self._ctx.event_publisher.emit(register_event)
        if opts.autostart:
            self._supervisor.ensure_running(descriptor, opts)

    def start(self, name: str) -> ServiceStatus | None:
        """🚀 Start a registered service.

        Args:
            name: The name of the service.

        Returns:
            ServiceStatus | None: The status of the service.

        Raises:
            RuntimeError: If the service is not registered.
        """
        descriptor = self._registry.get(name)
        options = self._registry.get_options(name)
        return self._supervisor.ensure_running(descriptor, options)

    def stop(
        self,
        name: str,
        *,
        timeout: float | None = None,
        reason: str | None = None,
    ) -> ServiceStatus | None:
        """🔄 Stop a registered service.

        Args:
            name: The name of the service.
            timeout: The timeout for the service to stop.
            reason: The reason for the service to stop.

        Raises:
            RuntimeError: If the service is not registered.
        """
        return self._supervisor.stop(name, timeout=timeout, reason=reason)

    def restart(self, name: str) -> ServiceStatus | None:
        """🔄 Restart a registered service.

        Args:
            name: The name of the service.

        Returns:
            ServiceStatus | None: The status of the service.

        Raises:
            RuntimeError: If the service is not registered.
        """
        status = self.stop(
            name, timeout=self._registry.get_options(name).graceful_timeout
        )
        if status is None:
            return None
        return self.start(name)

    def status(self, name: str) -> ServiceStatus | None:
        """🔍 Get the status of a registered service.

        Args:
            name: The name of the service.

        Returns:
            ServiceStatus | None: The status of the service.

        Raises:
            RuntimeError: If the service is not registered.
        """
        return self._supervisor.status(name)

    def list(self) -> list[ServiceOverview]:
        """🔍 Get the list of registered services.

        Returns:
            list[ServiceOverview]: The list of registered services.

        Raises:
            RuntimeError: If the service is not registered.
        """
        overview: list[ServiceOverview] = []
        for descriptor, options in self._registry.list():
            current = self._supervisor.status(descriptor.name)
            state = current.state if current else "stopped"
            overview.append(
                ServiceOverview(
                    name=descriptor.name,
                    state=state,
                    autostart=options.autostart,
                    restart_policy=options.restart_policy.strategy,
                    metadata=descriptor.metadata,
                )
            )
        return overview

    def remove(self, name: str, *, stop_running: bool = True) -> None:
        """🗑️ Remove a registered service.

        Args:
            name: The name of the service.
            stop_running: Whether to stop the service before removing it.

        Raises:
            RuntimeError: If the service is not registered.
        """
        if stop_running:
            self.stop(name, timeout=self._registry.get_options(name).graceful_timeout)
        self._registry.remove(name)


__all__ = ["ServicesFacade"]
