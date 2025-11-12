from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from threading import Event
from typing import Callable, Mapping

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._events import RuntimeEventTopic, make_event


@dataclass
class ServiceContext:
    """🕹️ Runtime controls exposed to service callables."""

    name: str
    metadata: Mapping[str, object]
    runtime_context: RuntimeContext
    start_time: datetime
    restart_count: int
    heartbeat_callback: Callable[[Mapping[str, object] | None], None]
    _stop_event: Event = field(default_factory=Event)

    def stop_requested(self) -> bool:
        """🔔 Check if the service has been requested to stop."""
        return self._stop_event.is_set()

    def request_stop(self) -> None:
        """🔔 Request the service to stop."""
        self._stop_event.set()

    def sleep(self, seconds: float) -> None:
        """🔔 Sleep for the given number of seconds.

        Args:
            seconds: The number of seconds to sleep.

        Returns:
            bool: ``True`` if the service should continue running after the sleep,
                ``False`` if the stop event was triggered.

        Raises:
            RuntimeError: If the service is not found.
        """
        self._stop_event.wait(timeout=seconds)

    def emit_heartbeat(
        self,
        detail: Mapping[str, object] | None = None,
    ) -> None:
        """🔔 Emit a heartbeat event.

        Args:
            detail: The detail of the heartbeat.

        Raises:
            RuntimeError: If the event is not found.
        """
        event = make_event(
            RuntimeEventTopic.SERVICE_HEARTBEAT,
            timestamp_factory=self.runtime_context.timestamp,
            payload={
                "workload_kind": "service",
                "workload_name": self.name,
                "metadata": self.metadata,
                "detail": detail or {},
            },
        )
        self.runtime_context.event_publisher.emit(event)
        self.heartbeat_callback(detail)
