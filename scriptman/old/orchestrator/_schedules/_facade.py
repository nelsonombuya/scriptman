from __future__ import annotations

from typing import Any, Mapping

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._events import RuntimeEventTopic, make_event
from scriptman.orchestrator._logging import (
    WorkloadLoggingOptions,
    extract_logging_options,
)
from scriptman.orchestrator._schedules import ScheduleTrigger
from scriptman.orchestrator._services import ServiceOptions, ServicesFacade

from ._queue import ScheduleQueue
from ._registry import ScheduleRegistry
from ._runner import SchedulerRunner
from ._types import SchedulableCallable, ScheduleEntry, ScheduleOptions


class SchedulesFacade:
    """⏱️ High-level API for registering and managing schedules."""

    def __init__(
        self,
        *,
        context: RuntimeContext,
        registry: ScheduleRegistry | None = None,
        queue: ScheduleQueue | None = None,
        services: ServicesFacade | None = None,
    ) -> None:
        """🔄 Initialize the schedules facade.

        Args:
            context: The runtime context.
            registry: The schedule registry.
            queue: The schedule queue.
            services: The services facade.
        """
        self._ctx = context
        self._registry = registry or ScheduleRegistry()
        self._queue = queue or ScheduleQueue()
        self._services = services
        self._runner = SchedulerRunner(
            runtime_context=context,
            registry=self._registry,
            queue=self._queue,
        )
        self._service_registered = False

    def register(
        self,
        name: str,
        callable: SchedulableCallable,
        *,
        trigger: ScheduleTrigger,
        options: ScheduleOptions | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        """🔄 Register a new schedule.

        Args:
            name: The name of the schedule.
            callable: The callable to schedule.
            trigger: The trigger for the schedule.
            options: The options for the schedule.
            metadata: The metadata for the schedule.
        """
        entry = ScheduleEntry(
            name=name,
            target=callable,
            trigger=trigger,
            metadata=metadata or {},
        )
        opts = options or ScheduleOptions()
        default_logging = WorkloadLoggingOptions(
            path_template="logs/schedules/{workload}/{date}/{run_id}.log"
        )
        decorator_logging = extract_logging_options(callable)
        if entry.logging == default_logging and decorator_logging != default_logging:
            entry.logging = decorator_logging
        self._registry.add(entry, opts)
        self._emit(RuntimeEventTopic.SCHEDULED_JOB_REGISTERED, entry, {})

        next_fire = trigger.next_fire(self._ctx.timestamp())
        if next_fire is not None:
            self._queue.enqueue(entry, next_fire)

        self._ensure_service()

    def pause(self, name: str) -> None:
        """🔄 Pause a schedule.

        Args:
            name: The name of the schedule.
        """
        entry = self._registry.get(name)
        self._queue.remove(name)
        self._emit(
            RuntimeEventTopic.SCHEDULED_JOB_REGISTERED,
            entry,
            {"status": "paused"},
        )

    def resume(self, name: str) -> None:
        """🔄 Resume a schedule.

        Args:
            name: The name of the schedule.
        """
        entry = self._registry.get(name)
        next_fire = entry.trigger.next_fire(self._ctx.timestamp())
        if next_fire is not None:
            self._queue.enqueue(entry, next_fire)
        self._emit(
            RuntimeEventTopic.SCHEDULED_JOB_REGISTERED,
            entry,
            {"status": "resumed"},
        )
        self._ensure_service()

    def run_now(self, name: str) -> None:
        """🔄 Run a schedule now.

        Args:
            name: The name of the schedule.
        """
        entry = self._registry.get(name)
        self._queue.enqueue(entry, self._ctx.timestamp())
        self._ensure_service()

    def _emit(
        self,
        topic: RuntimeEventTopic,
        entry: ScheduleEntry,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """🔔 Emit an event for a schedule entry."""
        payload_data: dict[str, object] = {
            "workload_kind": "schedule",
            "workload_name": entry.name,
            "metadata": entry.metadata,
        }
        if extra:
            for key, value in extra.items():
                payload_data[key] = value
        event = make_event(
            topic,
            timestamp_factory=self._ctx.timestamp,
            payload=payload_data,
        )
        self._ctx.event_publisher.emit(event)

    def _ensure_service(self) -> None:
        """🔄 Ensure the scheduler runner service is registered."""
        if not self._services or self._service_registered:
            return
        self._services.register(
            "__scheduler_runner__",
            lambda ctx: self._runner.service(ctx),
            options=ServiceOptions(autostart=True, daemon=True),
        )
        self._service_registered = True


__all__ = ["SchedulesFacade"]
