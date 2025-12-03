from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock, RLock
from typing import Any, Callable, Mapping, MutableMapping, Protocol

from loguru import Logger, logger

from ._events import EventPublisher

# 🕒 Clock interface used by the Command Deck to produce timestamps.
Clock = Callable[[], datetime]


def utc_clock() -> datetime:
    """
    ⏱️ Default clock producing timezone-aware timestamps.

    Returns:
        datetime: The current timestamp.
    """

    return datetime.now(tz=timezone.utc)


class ExecutorStrategy(Protocol):
    """⚙️ Executor interface used by the Command Deck to submit work."""

    name: str

    def acquire(self) -> Any:
        """
        Acquire an executor strategy.

        Returns:
            Any: The acquired executor strategy.
        """
        ...

    def release(self) -> None:
        """
        Release an executor strategy.
        """
        ...


class ExecutorRegistry(Protocol):
    """📦 Registry that hands out executor strategies by logical name."""

    def get(self, name: str) -> ExecutorStrategy:
        """
        Get an executor strategy by name.

        Args:
            name: The name of the executor strategy to get.

        Returns:
            ExecutorStrategy: The executor strategy.
        """
        ...

    def register(self, strategy: ExecutorStrategy) -> None:
        """
        Register an executor strategy.

        Args:
            strategy: The executor strategy to register.
        """
        ...


@dataclass(slots=True)
class RuntimeLocks:
    """🔐 Shared lock inventory provided to Command Deck collaborators."""

    registry: Lock = field(default_factory=Lock)
    lifecycle: RLock = field(default_factory=RLock)
    telemetry: RLock = field(default_factory=RLock)


@dataclass(slots=True)
class RuntimeContext:
    """🚀 Snapshot of resources shared across tasks, services, and schedules."""

    config: Mapping[str, object]
    clock: Clock
    event_publisher: EventPublisher
    executor_registry: ExecutorRegistry
    logger: Logger
    locks: RuntimeLocks
    metadata: MutableMapping[str, object]

    def timestamp(self) -> datetime:
        """⏱️ Produce a fresh timestamp using the bound clock.

        Returns:
            datetime: The current timestamp.
        """
        return self.clock()


class RuntimeContextBuilder:
    """⚙️ Construct a RuntimeContext with sensible defaults for beginners."""

    def __init__(self) -> None:
        self._config: Mapping[str, object] = {}
        self._clock: Clock = utc_clock
        self._event_publisher: EventPublisher | None = None
        self._executor_registry: ExecutorRegistry | None = None
        self._logger = logger.bind(component="Ouroboros Runtime Orchestrator")
        self._metadata: MutableMapping[str, object] = {}

    def with_config(self, config: Mapping[str, object]) -> "RuntimeContextBuilder":
        self._config = config
        return self

    def with_clock(self, clock: Clock) -> "RuntimeContextBuilder":
        self._clock = clock
        return self

    def with_event_publisher(self, publisher: EventPublisher) -> "RuntimeContextBuilder":
        self._event_publisher = publisher
        return self

    def with_executor_registry(
        self, registry: ExecutorRegistry
    ) -> "RuntimeContextBuilder":
        self._executor_registry = registry
        return self

    def with_logger(self, bound_logger: Logger) -> "RuntimeContextBuilder":
        self._logger = bound_logger
        return self

    def with_metadata(
        self, metadata: MutableMapping[str, object]
    ) -> "RuntimeContextBuilder":
        self._metadata = metadata
        return self

    def build(self) -> RuntimeContext:
        if self._event_publisher is None:
            raise RuntimeError("⚠️ Event publisher is required before building context")
        if self._executor_registry is None:
            raise RuntimeError("⚠️ Executor registry is required before building context")
        return RuntimeContext(
            config=self._config,
            clock=self._clock,
            event_publisher=self._event_publisher,
            executor_registry=self._executor_registry,
            logger=self._logger,
            locks=RuntimeLocks(),
            metadata=self._metadata,
        )


__all__ = [
    "Clock",
    "ExecutorRegistry",
    "ExecutorStrategy",
    "RuntimeContext",
    "RuntimeContextBuilder",
    "RuntimeLocks",
    "utc_clock",
]
