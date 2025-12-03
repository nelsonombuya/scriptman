from __future__ import annotations

from enum import Enum, auto
from typing import Callable, Iterable, Mapping, MutableMapping

from loguru import logger

from ._context import ExecutorRegistry, RuntimeContext, RuntimeContextBuilder, utc_clock
from ._events import EventPublisher, RuntimeEventTopic, make_event

# 🔄 Type alias for lifecycle hooks.
LifecycleHook = Callable[[RuntimeContext], None]


class RuntimePhase(Enum):
    """🔄 State machine phases for the Command Deck lifecycle."""

    CREATED = auto()
    BOOTSTRAPPED = auto()
    HYDRATED = auto()
    RUNTIME = auto()
    SHUTDOWN = auto()


class RuntimeOrchestrator:
    """🚀 Coordinate Scriptman runtime resources across tasks, services, and schedules."""

    def __init__(
        self,
        *,
        config: Mapping[str, object] | None = None,
        event_publisher: EventPublisher | None = None,
        executor_registry: ExecutorRegistry | None = None,
        metadata: MutableMapping[str, object] | None = None,
        hooks: Iterable[LifecycleHook] | None = None,
    ) -> None:
        self._config = config or {}
        self._event_publisher = event_publisher
        self._executor_registry = executor_registry
        self._metadata = metadata or {}
        self._hooks = tuple(hooks or ())
        self._context: RuntimeContext | None = None
        self._phase = RuntimePhase.CREATED
        self._logger = logger.bind(component="runtime-orchestrator")

    @property
    def context(self) -> RuntimeContext:
        """🔍 Access the live runtime context.

        Returns:
            RuntimeContext: The runtime context.
        """
        if self._context is None:
            raise RuntimeError(
                "⚠️ Runtime context requested before Ouroboros Runtime Orchestrator "
                "finished bootstrapping"
            )
        return self._context

    @property
    def phase(self) -> RuntimePhase:
        """🔍 Current lifecycle phase."""
        return self._phase

    def bootstrap(self) -> None:
        """🚀 Build foundational resources and move into BOOTSTRAPPED phase.

        Raises:
            RuntimeError: If the orchestrator is not in the CREATED phase.
        """
        if self._phase is not RuntimePhase.CREATED:
            raise RuntimeError("⚠️ bootstrap() may only be called once from CREATED")

        builder = RuntimeContextBuilder().with_config(self._config).with_clock(utc_clock)

        if self._event_publisher is None:
            raise RuntimeError("⚠️ RuntimeOrchestrator requires an event publisher")
        builder.with_event_publisher(self._event_publisher)

        if self._executor_registry is None:
            raise RuntimeError("⚠️ RuntimeOrchestrator requires an executor registry")
        builder.with_executor_registry(self._executor_registry)

        builder.with_metadata(self._metadata)

        self._context = builder.build()
        self._phase = RuntimePhase.BOOTSTRAPPED
        self._emit(RuntimeEventTopic.ORCHESTRATOR_BOOTSTRAPPED)

    def hydrate(self) -> None:
        """🔄 Perform stateful hydration after bootstrap.

        Raises:
            RuntimeError: If the orchestrator is not in the BOOTSTRAPPED phase.
        """
        self._require_phase(RuntimePhase.BOOTSTRAPPED, "hydrate")
        self._phase = RuntimePhase.HYDRATED
        self._emit(RuntimeEventTopic.ORCHESTRATOR_HYDRATED)

    def enter_runtime(self) -> None:
        """🚦 Transition into active runtime operations.

        Raises:
            RuntimeError: If the orchestrator is not in the HYDRATED phase.
        """
        self._require_phase(RuntimePhase.HYDRATED, "enter_runtime")
        for hook in self._hooks:
            hook(self.context)
        self._phase = RuntimePhase.RUNTIME
        self._emit(RuntimeEventTopic.ORCHESTRATOR_RUNTIME_ENTERED)

    def shutdown(self) -> None:
        """🧹 Gracefully shut down the Command Deck.

        Raises:
            RuntimeError: If the orchestrator is not in the RUNTIME phase.
        """
        if self._phase is RuntimePhase.SHUTDOWN:
            return
        if self._context is None:
            raise RuntimeError("⚠️ RuntimeOrchestrator shutdown invoked before bootstrap")
        self._emit(RuntimeEventTopic.ORCHESTRATOR_SHUTDOWN)
        self._phase = RuntimePhase.SHUTDOWN

    def _require_phase(self, phase: RuntimePhase, action: str) -> None:
        """🔍 Require the orchestrator to be in a specific phase.

        Args:
            phase: The phase to require.
            action: The action that requires the phase.

        Raises:
            RuntimeError: If the orchestrator is not in the required phase.
        """
        if self._phase is not phase:
            raise RuntimeError(
                f"⚠️ Cannot {action} while in phase {self._phase.name.lower()}"
            )

    def _emit(
        self,
        topic: RuntimeEventTopic | str,
        *,
        payload: Mapping[str, object] | None = None,
        **extras: object,
    ) -> None:
        """🔍 Emit an event.

        Args:
            topic: The topic of the event.
            payload: Optional structured payload.
            **extras: Additional key-value fields merged into the payload.
        """
        if self._context is None:
            return
        event = make_event(
            topic,
            timestamp_factory=self._context.timestamp,
            payload=payload,
            **extras,
        )
        self._context.event_publisher.emit(event)


__all__ = ["RuntimeOrchestrator", "RuntimePhase"]
