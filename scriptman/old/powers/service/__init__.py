"""
⚒ Task service infrastructure for long-running cooperative loops.

This module provides a lightweight service manager that allows components to
register long-running loops which should stay alive for the lifetime of the
application. Services are executed in dedicated threads and receive a
``ServiceContext`` instance that exposes cooperative cancellation primitives
and access to the ``TaskManager`` instance.

The design mirrors the queuing pattern used by ``scriptman.powers.api``;
services can be registered before the manager is instantiated; registrations
are queued until the ``TaskManager`` is ready to spin them up.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event, RLock, Thread
from time import monotonic
from typing import TYPE_CHECKING, Any, Callable, Optional

from loguru import logger

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
    from scriptman.powers.tasks import TaskManager


ServiceCallable = Callable[["ServiceContext"], None]


@dataclass(frozen=True, slots=True)
class ServiceDefinition:
    """⚙️ Configuration for a long-running service loop."""

    name: str
    target: ServiceCallable
    daemon: bool = True
    autostart: bool = True
    keep_alive: bool = True
    restart_delay: float = 5.0

    def __post_init__(self) -> None:
        if not self.name or self.name.isspace():  # pragma: no cover - sanity check
            raise ValueError("Service name cannot be empty")
        if self.restart_delay < 0:
            raise ValueError("restart_delay must be zero or greater")


@dataclass(slots=True)
class ServiceRuntime:
    """🔄 Runtime metadata for a registered service."""

    definition: ServiceDefinition
    stop_event: Event = field(default_factory=Event)
    thread: Optional[Thread] = None
    restarts: int = 0
    last_error: Optional[BaseException] = None
    last_started_at: Optional[float] = None
    lock: RLock = field(default_factory=RLock)


class ServiceContext:
    """🔍 Context object provided to running services."""

    __slots__ = ("_task_manager", "_stop_event", "name")

    def __init__(
        self,
        *,
        task_manager: "TaskManager",
        stop_event: Event,
        name: str,
    ) -> None:
        """🔍 Initialize the service context."""

        self._task_manager = task_manager
        self._stop_event = stop_event
        self.name = name

    @property
    def task_manager(self) -> "TaskManager":
        """🔍 Reference back to the owning ``TaskManager`` instance."""

        return self._task_manager

    @property
    def stop_event(self) -> Event:
        """🔔 Event that is signalled when the service should terminate."""

        return self._stop_event

    @property
    def should_stop(self) -> bool:
        """🔔 Whether the service has been asked to stop."""

        return self._stop_event.is_set()

    def sleep(self, seconds: float) -> bool:
        """
        ⏳ Sleep cooperatively until ``seconds`` elapse or a stop is requested.

        Args:
            seconds: The number of seconds to sleep.

        Returns:
            ``True`` if the service should continue running after the delay,
            or ``False`` if the stop event was triggered while waiting.
        """

        return not self._stop_event.wait(timeout=max(0.0, seconds))


class ServiceRegistry:
    """🔄 Global queue for service registrations before manager instantiation."""

    _queued: list[ServiceDefinition] = []
    _lock: RLock = RLock()

    @classmethod
    def queue(cls, definition: ServiceDefinition) -> None:
        """🔄 Queue a service registration."""

        with cls._lock:
            if any(d.name == definition.name for d in cls._queued):
                raise ValueError(f"Service '{definition.name}' already queued")
            cls._queued.append(definition)
            logger.debug(f"Queued service registration: {definition.name}")

    @classmethod
    def drain(cls) -> list[ServiceDefinition]:
        """🔄 Drain the service registration queue."""

        with cls._lock:
            queued = cls._queued[:]
            cls._queued.clear()
            return queued

    @classmethod
    def has_queued(cls) -> bool:
        """🔔 Whether there are any services queued for registration."""

        with cls._lock:
            return bool(cls._queued)


class ServiceManager:
    """🔄 Orchestrates lifecycle for long-running services."""

    def __init__(self, task_manager: "TaskManager") -> None:
        """🔄 Initialize the service manager."""

        self._lock = RLock()
        self._shutdown = False
        self._task_manager = task_manager
        self._services: dict[str, ServiceRuntime] = {}

        queued = ServiceRegistry.drain()
        for definition in queued:
            self._register_internal(definition)

        for definition in queued:
            if definition.autostart:
                self.start(definition.name)

    def _register_internal(self, definition: ServiceDefinition) -> ServiceRuntime:
        """🔄 Register a service internally."""

        with self._lock:
            if self._shutdown:
                raise RuntimeError("Cannot register services after shutdown")
            if definition.name in self._services:
                raise ValueError(f"Service '{definition.name}' already registered")
            runtime = ServiceRuntime(definition=definition)
            self._services[definition.name] = runtime
            logger.debug(f"🖊️ Registered service '{definition.name}'")
            return runtime

    def register(
        self,
        definition: ServiceDefinition,
        *,
        start: Optional[bool] = None,
    ) -> None:
        """🔄 Register a service with the service manager."""

        runtime = self._register_internal(definition)
        autostart = definition.autostart if start is None else start
        if autostart:
            self.start(runtime.definition.name)

    def has_service(self, name: str) -> bool:
        """🔔 Whether the service is registered."""

        with self._lock:
            return name in self._services

    def start(self, name: str) -> None:
        """🔄 Start a service."""

        runtime = self._services.get(name)
        if runtime is None:
            raise KeyError(f"Service '{name}' is not registered")

        with runtime.lock:
            if runtime.thread and runtime.thread.is_alive():
                logger.debug(f"🔔 Service '{name}' is already running")
                return

            runtime.stop_event.clear()
            runtime.thread = Thread(
                name=f"Task Service: {name}",
                target=self._run_service,
                args=(runtime,),
                daemon=runtime.definition.daemon,
            )
            runtime.last_started_at = monotonic()
            runtime.thread.start()
            logger.info(f"🔔 Service '{name}' started")

    def stop(self, name: str, *, timeout: Optional[float] = None) -> None:
        """🔄 Stop a service."""

        runtime = self._services.get(name)
        if runtime is None:
            raise KeyError(f"Service '{name}' is not registered")

        with runtime.lock:
            if runtime.thread is None:
                runtime.stop_event.set()
                return

            runtime.stop_event.set()
            thread = runtime.thread

        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
            logger.info(f"🔔 Service '{name}' stopped")

        with runtime.lock:
            runtime.thread = None

    def start_all(self) -> None:
        """🔄 Start all registered services."""

        for name in list(self._services.keys()):
            try:
                self.start(name)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(f"Failed to start service '{name}'")

    def stop_all(self, *, timeout: Optional[float] = None) -> None:
        """🔄 Stop all registered services."""

        for name in list(self._services.keys()):
            try:
                self.stop(name, timeout=timeout)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(f"Failed to stop service '{name}'")

    def shutdown(self, *, wait: bool = True, timeout: Optional[float] = None) -> None:
        """🔄 Shutdown the service manager."""

        with self._lock:
            self._shutdown = True

        self.stop_all(timeout=timeout if wait else 0.0)
        logger.debug("Service manager shutdown complete")

    def has_running_services(self) -> bool:
        """🔔 Whether there are any running services."""

        return any(
            runtime.thread and runtime.thread.is_alive()
            for runtime in self._services.values()
        )

    def _run_service(self, runtime: ServiceRuntime) -> None:
        """🔄 Run a service."""

        name = runtime.definition.name
        context = ServiceContext(
            task_manager=self._task_manager,
            stop_event=runtime.stop_event,
            name=name,
        )

        while not runtime.stop_event.is_set():
            try:
                runtime.definition.target(context)
                runtime.last_error = None
                if not runtime.definition.keep_alive:
                    break
            except Exception as exc:  # noqa: BLE001
                runtime.last_error = exc
                logger.exception(f"Service '{name}' encountered an error: {exc}")
                if not runtime.definition.keep_alive:
                    break

            if runtime.stop_event.is_set():
                break

            runtime.restarts += 1
            delay = runtime.definition.restart_delay
            if delay > 0 and runtime.stop_event.wait(delay):
                break

        logger.info(f"Service '{name}' loop exited")

        with runtime.lock:
            runtime.thread = None


class _ServiceManagerProxy:
    """🧲 Proxy that delegates to ``TaskManager().services``."""

    def _resolve(self) -> ServiceManager | None:
        return TaskManager().services

    def __getattr__(self, item: str) -> Any:  # pragma: no cover - delegation only
        manager = self._resolve()
        if manager is None:
            raise AttributeError("Service manager has not been initialised yet")
        return getattr(manager, item)

    def __bool__(self) -> bool:  # pragma: no cover - simple helper
        return self._resolve() is not None


service_manager = _ServiceManagerProxy()


__all__ = [
    "ServiceCallable",
    "ServiceContext",
    "ServiceDefinition",
    "ServiceManager",
    "ServiceRegistry",
    "service_manager",
]
