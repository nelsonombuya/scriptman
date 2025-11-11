from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from threading import RLock
from typing import Callable, DefaultDict, Iterable, Protocol


class RuntimeEventTopic(str, Enum):
    """🔖 Canonical event topics emitted by the Ouroboros Runtime Orchestrator."""

    ORCHESTRATOR_BOOTSTRAPPED = "orchestrator.bootstrapped"
    ORCHESTRATOR_HYDRATED = "orchestrator.hydrated"
    ORCHESTRATOR_RUNTIME_ENTERED = "orchestrator.runtime.entered"
    ORCHESTRATOR_SHUTDOWN = "orchestrator.shutdown"
    TASK_REGISTERED = "tasks.registered"
    TASK_ENQUEUED = "tasks.enqueued"
    TASK_STARTED = "tasks.started"
    TASK_RETRYING = "tasks.retrying"
    TASK_COMPLETED = "tasks.completed"
    TASK_FAILED = "tasks.failed"
    SERVICE_REGISTERED = "services.registered"
    SERVICE_STARTED = "services.started"
    SERVICE_STOPPED = "services.stopped"
    SCHEDULED_JOB_REGISTERED = "schedules.job.registered"
    SCHEDULED_JOB_TRIGGERED = "schedules.job.triggered"
    TELEMETRY_FLUSHED = "telemetry.flushed"


@dataclass(slots=True)
class RuntimeEvent:
    """🔍 Event payload carrying structured information to observers."""

    topic: RuntimeEventTopic | str
    payload: dict[str, object]
    timestamp: datetime


class RuntimeEventSubscriber(Protocol):
    """👂 Listener contract for Command Deck events."""

    def handle(self, event: RuntimeEvent) -> None:
        """
        Handle an event.

        Args:
            event: The event to handle.
        """
        ...


class EventPublisher(Protocol):
    """📡 Publish runtime events to subscribers."""

    def emit(self, event: RuntimeEvent) -> None:
        """
        Emit an event.

        Args:
            event: The event to emit.
        """
        ...


class SynchronousEventBus(EventPublisher):
    """🔄 Simple in-process dispatcher with subscription management."""

    def __init__(self) -> None:
        self._subscribers: DefaultDict[str, list[RuntimeEventSubscriber]] = defaultdict(
            list
        )
        self._lock = RLock()

    def subscribe(
        self,
        topics: Iterable[str | RuntimeEventTopic],
        subscriber: RuntimeEventSubscriber,
    ) -> None:
        """✍️ Register a subscriber for one or more topics.

        Args:
            topics: The topics to subscribe to.
            subscriber: The subscriber to register.
        """
        with self._lock:
            for topic in topics:
                self._subscribers[str(topic)].append(subscriber)

    def unsubscribe(
        self,
        topics: Iterable[str | RuntimeEventTopic],
        subscriber: RuntimeEventSubscriber,
    ) -> None:
        """🧹 Remove a subscriber from one or more topics.

        Args:
            topics: The topics to unsubscribe from.
            subscriber: The subscriber to unsubscribe.
        """
        with self._lock:
            for topic in topics:
                bucket = self._subscribers.get(str(topic))
                if not bucket:
                    continue
                self._subscribers[str(topic)] = [
                    candidate for candidate in bucket if candidate is not subscriber
                ]

    def emit(self, event: RuntimeEvent) -> None:
        """📡 Dispatch an event synchronously to registered subscribers.

        Args:
            event: The event to emit.
        """
        topic_key = str(event.topic)
        with self._lock:
            subscribers = list(self._subscribers.get(topic_key, ()))
            wildcard = list(self._subscribers.get("*", ()))
        for subscriber in subscribers + wildcard:
            subscriber.handle(event)


class EventCapture(RuntimeEventSubscriber):
    """🔍 Lightweight subscriber that records events for assertions."""

    def __init__(self) -> None:
        self._events: list[RuntimeEvent] = []

    def handle(self, event: RuntimeEvent) -> None:
        self._events.append(event)

    @property
    def events(self) -> list[RuntimeEvent]:
        """🔍 Access captured events."""
        return list(self._events)


def make_event(
    topic: RuntimeEventTopic | str,
    *,
    timestamp_factory: Callable[[], datetime],
    **payload: object,
) -> RuntimeEvent:
    """🪄 Helper to construct runtime events with consistent timestamps.

    Args:
        topic: The topic of the event.
        timestamp_factory: The factory to produce the timestamp.
        **payload: The payload of the event.

    Returns:
        RuntimeEvent: The constructed event.
    """
    return RuntimeEvent(topic=topic, payload=dict(payload), timestamp=timestamp_factory())


__all__ = [
    "EventCapture",
    "EventPublisher",
    "RuntimeEvent",
    "RuntimeEventSubscriber",
    "RuntimeEventTopic",
    "SynchronousEventBus",
    "make_event",
]
