from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from threading import Event, Lock, Thread
from typing import Mapping, MutableMapping
from uuid import uuid4

from loguru import logger

from scriptman.orchestrator._context import RuntimeContext
from scriptman.orchestrator._events import RuntimeEventTopic, make_event
from scriptman.orchestrator._logging import workload_log_sink
from scriptman.orchestrator._workloads import WorkloadOutcome

from ._context import ServiceContext
from ._facade_types import ServiceEntry, ServiceOptions
from ._policies import RestartPolicy
from ._status import ServiceStatus


@dataclass
class ServiceProcess:
    entry: ServiceEntry
    options: ServiceOptions
    context: ServiceContext
    thread: Thread
    restart_count: int = 0
    last_heartbeat: datetime | None = None
    stop_event: Event = field(default_factory=Event)
    retry_count: int = 0
    last_failure: datetime | None = None


class ServiceSupervisor:
    """🧠 Orchestrate service lifecycle, restarts, and telemetry."""

    def __init__(self, runtime_context: RuntimeContext) -> None:
        self._ctx = runtime_context
        self._services: MutableMapping[str, ServiceProcess] = {}
        self._lock = Lock()

    def ensure_running(self, entry: ServiceEntry, options: ServiceOptions) -> ServiceStatus:
        """🔄 Ensure a service is running.

        Args:
            entry: The service entry.
            options: The service options.

        Returns:
            ServiceStatus: The status of the service.
        """
        with self._lock:
            if entry.name in self._services:
                process = self._services[entry.name]
                if process.thread.is_alive():
                    return self._status_for(process)
                # restart stale thread
                process.restart_count += 1
                self._launch(process)
                return self._status_for(process)

            context = ServiceContext(
                name=entry.name,
                metadata=options.metadata,
                runtime_context=self._ctx,
                start_time=self._ctx.timestamp(),
                restart_count=0,
                heartbeat_callback=lambda detail: self._record_heartbeat(
                    entry.name, detail
                ),
            )
            process = ServiceProcess(
                entry=entry,
                options=options,
                context=context,
                thread=self._build_thread(entry, context, options),
            )
            self._services[entry.name] = process
            self._emit(RuntimeEventTopic.SERVICE_STARTED, process, {"status": "starting"})
            process.thread.start()
            return self._status_for(process)

    def stop(
        self,
        name: str,
        *,
        timeout: float | None = None,
        reason: str | None = None,
    ) -> ServiceStatus | None:
        """🔄 Stop a service.

        Args:
            name: The name of the service.
            timeout: The timeout for the service to stop.
            reason: The reason for the service to stop.

        Returns:
            ServiceStatus | None: The status of the service.
        """
        with self._lock:
            process = self._services.get(name)
            if not process:
                return None
            process.context.request_stop()
            process.stop_event.set()
            process.thread.join(timeout=timeout)
            if process.thread.is_alive():
                self._emit(
                    RuntimeEventTopic.SERVICE_STOPPED,
                    process,
                    {"status": "timeout", "reason": reason or "timeout"},
                )
            else:
                self._emit(
                    RuntimeEventTopic.SERVICE_STOPPED,
                    process,
                    {"status": "stopped", "reason": reason or "requested"},
                )
            return self._status_for(process)

    def status(self, name: str) -> ServiceStatus | None:
        """🔍 Get the status of a service.

        Args:
            name: The name of the service.

        Returns:
            ServiceStatus | None: The status of the service.
        """
        with self._lock:
            process = self._services.get(name)
            if not process:
                return None
            return self._status_for(process)

    def broadcast_stop(self) -> None:
        """🔔 Broadcast a stop signal to all services."""
        with self._lock:
            for process in self._services.values():
                process.context.request_stop()
                process.stop_event.set()

    def _record_heartbeat(
        self,
        name: str,
        detail: Mapping[str, object] | None,
    ) -> None:
        """🔔 Record a heartbeat for a service.

        Args:
            name: The name of the service.
            detail: The detail of the heartbeat.
        """
        with self._lock:
            process = self._services.get(name)
            if not process:
                return
            process.last_heartbeat = self._ctx.timestamp()

    # internal helpers
    def _build_thread(
        self,
        entry: ServiceEntry,
        context: ServiceContext,
        options: ServiceOptions,
    ) -> Thread:
        """🔄 Build a thread for a service.

        Args:
            entry: The service entry.
            context: The service context.
            options: The service options.

        Returns:
            Thread: The thread for the service.
        """
        return Thread(
            target=self._run_service,
            name=f"service:{entry.name}",
            args=(entry, context, options),
            daemon=options.daemon,
        )

    def _launch(self, process: ServiceProcess) -> None:
        """🔄 Launch a service.

        Args:
            process: The service process.
        """
        process.stop_event.clear()
        process.context = ServiceContext(
            name=process.entry.name,
            metadata=process.options.metadata,
            runtime_context=self._ctx,
            start_time=self._ctx.timestamp(),
            restart_count=process.restart_count,
            heartbeat_callback=lambda detail: self._record_heartbeat(
                process.entry.name, detail
            ),
        )
        process.thread = self._build_thread(
            process.entry,
            process.context,
            process.options,
        )
        self._emit(
            RuntimeEventTopic.SERVICE_STARTED,
            process,
            {"status": "restarting", "restart_count": process.restart_count},
        )
        process.thread.start()

    def _run_service(
        self,
        entry: ServiceEntry,
        context: ServiceContext,
        options: ServiceOptions,
    ) -> None:
        """🔄 Run a service.

        Args:
            entry: The service entry.
            context: The service context.
            options: The service options.
        """
        run_id = f"{entry.name}-{context.restart_count}-{uuid4()}"
        with workload_log_sink(
            workload=entry.name,
            run_id=run_id,
            options=entry.logging,
        ):
            try:
                result = entry.target(context)
                if asyncio.iscoroutine(result):
                    asyncio.run(result)
                self._emit(
                    RuntimeEventTopic.SERVICE_STOPPED,
                    self._services[entry.name],
                    {"status": "exited", "run_id": run_id},
                )
            except Exception as exc:  # pragma: no cover - runtime path
                logger.exception("Service %s crashed: %s", entry.name, exc)
                payload = {
                    "status": "failed",
                    "error": str(exc),
                    "run_id": run_id,
                }
                self._emit(
                    RuntimeEventTopic.SERVICE_FAILED,
                    self._services[entry.name],
                    payload,
                )
                self._schedule_restart(entry.name, exc)

    def _status_for(self, process: ServiceProcess) -> ServiceStatus:
        """🔍 Get the status of a service.

        Args:
            process: The service process.

        Returns:
            ServiceStatus: The status of the service.
        """
        uptime = (self._ctx.timestamp() - process.context.start_time).total_seconds()
        state: WorkloadOutcome = "running" if process.thread.is_alive() else "stopped"
        return ServiceStatus(
            name=process.entry.name,
            state=state,
            uptime_seconds=uptime,
            restart_count=process.restart_count,
            last_heartbeat=process.last_heartbeat,
            detail=process.options.metadata,
        )

    def _emit(
        self,
        topic: RuntimeEventTopic,
        process: ServiceProcess,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """🔔 Emit an event for a service.

        Args:
            topic: The topic of the event.
            process: The service process.
            extra: The extra of the event.
        """
        payload: dict[str, object] = {
            "workload_kind": "service",
            "workload_name": process.entry.name,
            "metadata": process.options.metadata,
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

    def _schedule_restart(self, name: str, exc: Exception) -> None:
        """🔔 Schedule a restart for a service.

        Args:
            name: The name of the service.
            exc: The exception that caused the service to fail.
        """
        with self._lock:
            process = self._services.get(name)
            if not process:
                return
            policy = process.options.restart_policy
            process.last_failure = self._ctx.timestamp()

            if policy.strategy == "never":
                return
            if policy.strategy == "limited":
                if (
                    policy.max_retries is not None
                    and process.retry_count >= policy.max_retries
                ):
                    return
                process.retry_count += 1

            delay = self._compute_backoff(process.restart_count, policy)
            self._emit(
                RuntimeEventTopic.SERVICE_RETRYING,
                process,
                {"backoff_seconds": delay, "error": str(exc)},
            )
            if policy.on_failure:
                policy.on_failure(
                    {
                        "workload_kind": "service",
                        "workload_name": name,
                        "metadata": process.options.metadata,
                        "error": str(exc),
                    }
                )

        if delay > 0:
            if process.stop_event.wait(delay):
                return

        with self._lock:
            process = self._services.get(name)
            if not process:
                return
            process.restart_count += 1
            process.retry_count = 0
            self._launch(process)

    def _compute_backoff(
        self,
        restart_count: int,
        policy: RestartPolicy,
    ) -> float:
        """🔔 Compute the backoff for a service.

        Args:
            restart_count: The number of times the service has restarted.
            policy: The restart policy.

        Returns:
            float: The backoff time.
        """
        if policy.strategy == "never":
            return 0.0
        base = policy.backoff.initial_delay
        delay = base * (policy.backoff.multiplier ** max(0, restart_count - 1))
        return min(delay, policy.backoff.max_delay)


__all__ = ["ServiceSupervisor"]
