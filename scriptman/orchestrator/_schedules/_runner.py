from __future__ import annotations

import asyncio
import inspect
from functools import wraps
from typing import Any, Awaitable, Callable, Mapping
from uuid import uuid4

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._events import RuntimeEventTopic, make_event
from scriptman.orchestrator._logging import workload_log_sink
from scriptman.orchestrator._services._context import ServiceContext

from ._queue import QueueEntry, ScheduleQueue
from ._registry import ScheduleRegistry
from ._types import ScheduleDescriptor


class SchedulerRunner:
    """⏱️ Background loop that activates schedules."""

    def __init__(
        self,
        *,
        runtime_context: RuntimeContext,
        registry: ScheduleRegistry,
        queue: ScheduleQueue,
    ) -> None:
        self._ctx = runtime_context
        self._registry = registry
        self._queue = queue

    def service(self, service_context: ServiceContext) -> None:
        """🔁 Background loop for the scheduler runner service.

        Args:
            service_context: The service context.
        """

        while not service_context.stop_requested():
            now = self._ctx.timestamp()
            due = self._queue.dequeue_due(now)
            if due is None:
                next_entry = self._queue.next_entry()
                wait_seconds = (
                    (next_entry.fire_time - now).total_seconds() if next_entry else 5.0
                )
                service_context.sleep(max(wait_seconds, 0.5))
                continue

            self._handle_entry(due, service_context)

    def _handle_entry(
        self,
        entry: QueueEntry,
        service_context: ServiceContext,
    ) -> None:
        """🔄 Handle a schedule entry.

        Args:
            entry: The schedule entry.
            service_context: The service context.
        """
        descriptor = entry.descriptor

        self._emit(
            RuntimeEventTopic.SCHEDULED_JOB_TRIGGERED,
            descriptor,
            {
                "planned_time": entry.fire_time.isoformat(),
                "actual_time": self._ctx.timestamp().isoformat(),
            },
        )

        invocation = self._wrap_target(descriptor)
        result = invocation()
        if inspect.isawaitable(result):
            asyncio.run(self._await_result(result))

        next_fire = descriptor.trigger.next_fire(entry.fire_time)
        self._queue.reschedule(descriptor, next_fire)

    def _wrap_target(self, descriptor: ScheduleDescriptor) -> Callable[..., Any]:
        """🔄 Wrap the target function with logging.

        Args:
            descriptor: The schedule descriptor.
        """
        target = descriptor.target
        options = descriptor.logging
        if not options.enabled:
            return target

        if inspect.iscoroutinefunction(target):  # async schedule callable

            @wraps(target)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                run_id = f"{descriptor.name}-{uuid4()}"
                with workload_log_sink(
                    workload=descriptor.name,
                    run_id=run_id,
                    options=options,
                    args=args if options.include_args else None,
                    kwargs=kwargs if options.include_args else None,
                ):
                    return await target(*args, **kwargs)

            return async_wrapper

        @wraps(target)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            run_id = f"{descriptor.name}-{uuid4()}"
            with workload_log_sink(
                workload=descriptor.name,
                run_id=run_id,
                options=options,
                args=args if options.include_args else None,
                kwargs=kwargs if options.include_args else None,
            ):
                result = target(*args, **kwargs)
                if inspect.isawaitable(result):
                    return asyncio.run(result)  # type: ignore[arg-type]
                return result

        return sync_wrapper

    def _emit(
        self,
        topic: RuntimeEventTopic,
        descriptor: ScheduleDescriptor,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        payload: dict[str, object] = {
            "workload_kind": "schedule",
            "workload_name": descriptor.name,
            "metadata": descriptor.metadata,
        }
        if extra:
            for key, value in extra.items():
                payload[key] = value
        event = make_event(
            topic,
            timestamp_factory=self._ctx.timestamp,
            payload=payload,
        )
        self._ctx.event_publisher.emit(event)

    @staticmethod
    async def _await_result(awaitable: Awaitable[Any]) -> Any:
        """🔄 Await the result of an awaitable.

        Args:
            awaitable: The awaitable to await.
        """
        return await awaitable


__all__ = ["SchedulerRunner"]
