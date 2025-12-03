"""⚙️ Lightweight scheduler service managed by the TaskManager."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, tzinfo
from threading import Event, RLock
from time import monotonic
from typing import Any, Callable, Optional

from loguru import logger

from scriptman.powers.service import ServiceContext

from .host import SchedulerHost
from .triggers import (
    IntervalTrigger,
    OneTimeTrigger,
    SchedulerTrigger,
    TimeOfDayTrigger,
)

SchedulerCallable = Callable[..., Any]


@dataclass(slots=True)
class ScheduledJob:
    """🗂 Internal job representation used by the scheduler service."""

    id: str
    func: SchedulerCallable
    trigger: SchedulerTrigger
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    max_instances: int = 1
    enabled: bool = True
    next_run: datetime = field(default_factory=datetime.now)
    running: int = 0

    def __post_init__(self) -> None:
        if self.max_instances < 1:
            raise ValueError("max_instances must be at least 1")


class SchedulerService:
    """⚙️ Scheduler loop that dispatches jobs through the TaskManager."""

    _SERVICE_NAME = "task-scheduler-service"

    def __init__(self, host: SchedulerHost) -> None:
        self._host = host
        self._jobs: dict[str, ScheduledJob] = {}
        self._lock = RLock()
        self._wake_event = Event()
        self._service_registered = False

    # ------------------------------------------------------------------
    # Public registration API
    # ------------------------------------------------------------------

    def add_interval_job(
        self,
        func: SchedulerCallable,
        *,
        job_id: str,
        every: timedelta,
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
        max_instances: int = 1,
        enabled: bool = True,
    ) -> ScheduledJob:
        """♻️ Add a job that repeats on a fixed interval.

        Args:
            func: Callable executed on each run.
            job_id: Unique job identifier.
            every: Interval between executions.
            args: Positional arguments passed to ``func``.
            kwargs: Keyword arguments passed to ``func``.
            max_instances: Maximum concurrent executions allowed.
            enabled: Whether the job should be active immediately.

        Returns:
            The registered :class:`ScheduledJob` snapshot.
        """

        trigger = IntervalTrigger(every)
        job = ScheduledJob(
            id=job_id,
            func=func,
            trigger=trigger,
            args=args or tuple(),
            kwargs=kwargs or {},
            max_instances=max_instances,
            enabled=enabled,
            next_run=trigger.next_run(None),
        )
        return self._add_job(job)

    def add_time_of_day_job(
        self,
        func: SchedulerCallable,
        *,
        job_id: str,
        at: time,
        timezone: Optional[tzinfo] = None,
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
        max_instances: int = 1,
        enabled: bool = True,
    ) -> ScheduledJob:
        """🌞 Add a job that runs every day at the given time.

        Args:
            func: Callable executed when the trigger fires.
            job_id: Unique job identifier.
            at: Time-of-day to invoke the job.
            timezone: Optional timezone for evaluation.
            args: Positional arguments passed to ``func``.
            kwargs: Keyword arguments passed to ``func``.
            max_instances: Maximum concurrent executions allowed.
            enabled: Whether the job should be active immediately.

        Returns:
            The registered :class:`ScheduledJob` snapshot.
        """

        trigger = TimeOfDayTrigger(at=at, timezone=timezone)
        job = ScheduledJob(
            id=job_id,
            func=func,
            trigger=trigger,
            args=args or tuple(),
            kwargs=kwargs or {},
            max_instances=max_instances,
            enabled=enabled,
            next_run=trigger.next_run(None),
        )
        return self._add_job(job)

    def add_one_time_job(
        self,
        func: SchedulerCallable,
        *,
        job_id: str,
        run_at: datetime,
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
        enabled: bool = True,
    ) -> ScheduledJob:
        """🎯 Add a job that should run only once at a specific datetime.

        Args:
            func: Callable executed at ``run_at``.
            job_id: Unique job identifier.
            run_at: Exact :class:`datetime` for the single execution.
            args: Positional arguments passed to ``func``.
            kwargs: Keyword arguments passed to ``func``.
            enabled: Whether the job should be active immediately.

        Returns:
            The registered :class:`ScheduledJob` snapshot.
        """

        trigger = OneTimeTrigger(run_at=run_at)
        job = ScheduledJob(
            id=job_id,
            func=func,
            trigger=trigger,
            args=args or tuple(),
            kwargs=kwargs or {},
            max_instances=1,
            enabled=enabled,
            next_run=trigger.next_run(None),
        )
        return self._add_job(job)

    def remove_job(self, job_id: str) -> bool:
        with self._lock:
            removed = self._jobs.pop(job_id, None) is not None
        if removed:
            logger.info(f"Removed scheduled job '{job_id}'")
            self._wake_event.set()
        return removed

    def list_jobs(self) -> list[ScheduledJob]:
        """📋 Return copies of all scheduled jobs."""

        with self._lock:
            return [deepcopy(job) for job in self._jobs.values()]

    def set_job_enabled(self, job_id: str, enabled: bool) -> bool:
        """🔔 Enable or disable a job.

        Args:
            job_id: Identifier of the job to modify.
            enabled: Desired enabled state.

        Returns:
            ``True`` if the job exists and was updated, otherwise ``False``.
        """

        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False

            job.enabled = enabled
            if enabled:
                job.next_run = job.trigger.next_run(None)
            self._wake_event.set()
            return True

    def update_job_trigger(self, job_id: str, trigger: SchedulerTrigger) -> bool:
        """🔄 Replace a job's trigger.

        Args:
            job_id: Identifier of the job to modify.
            trigger: New trigger controlling execution schedule.

        Returns:
            ``True`` if the job exists and the trigger was replaced.
        """

        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False

            job.trigger = trigger
            job.next_run = trigger.next_run(None)
            self._wake_event.set()
            return True

    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        """🔍 Retrieve a copy of the registered job metadata."""

        with self._lock:
            job = self._jobs.get(job_id)
            return deepcopy(job) if job else None

    # ------------------------------------------------------------------
    # Internal operations
    # ------------------------------------------------------------------

    def _add_job(self, job: ScheduledJob) -> ScheduledJob:
        """🧾 Register the job and ensure the service loop is active."""

        with self._lock:
            if job.id in self._jobs:
                raise ValueError(f"Job '{job.id}' is already registered")
            self._jobs[job.id] = job
            logger.info(
                "Scheduled job '%s' (next run: %s)", job.id, job.next_run.isoformat()
            )

        self._ensure_service_registered()
        self._wake_event.set()
        return job

    def _ensure_service_registered(self) -> None:
        """🛎️ Ensure the background service loop is ready to dispatch jobs."""

        if self._service_registered:
            return
        try:
            self._host.register_service(
                self._SERVICE_NAME,
                self._service_loop,
                autostart=True,
                daemon=False,
                keepalive=True,
                restart_delay=1.0,
            )
        except ValueError:
            logger.debug("Scheduler service already registered")
        finally:
            self._service_registered = True
            self._wake_event.set()

    def _service_loop(self, context: ServiceContext) -> None:
        """🔁 Background loop dispatching jobs according to triggers."""

        logger.info("Scheduler service loop started")

        try:
            while True:
                if context.should_stop:
                    break
                job_id, wait_time = self._next_job()

                if job_id is None:
                    if not self._wait(context, 1.0):
                        break
                    continue

                if wait_time > 0 and not self._wait(context, wait_time):
                    break

                dispatched = self._dispatch(job_id)
                if not dispatched and not self._wait(context, 0.5):
                    break

        except Exception as exc:  # noqa: BLE001
            logger.exception(f"Scheduler service encountered an error: {exc}")
        finally:
            logger.info("Scheduler service loop exiting")

    def _next_job(self) -> tuple[Optional[str], float]:
        """⏭ Determine the next job to run plus wait duration."""

        with self._lock:
            enabled_jobs = [item for item in self._jobs.items() if item[1].enabled]
            if not enabled_jobs:
                return None, 0.0

            job_id, job = min(enabled_jobs, key=lambda item: item[1].next_run)

        now = self._now_for_job(job)
        wait = max(0.0, (job.next_run - now).total_seconds())
        return job_id, wait

    def _dispatch(self, job_id: str) -> bool:
        """🚀 Submit the job to the TaskManager for execution."""

        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or not job.enabled or job.running >= job.max_instances:
                return False

            scheduled_time = job.next_run
            job.running += 1
            if isinstance(job.trigger, OneTimeTrigger):
                job.enabled = False
            job.next_run = job.trigger.next_run(scheduled_time)
            args = job.args
            kwargs = job.kwargs
            func = job.func

        task = self._host.background(func, *args, **kwargs)

        def _on_complete(_future: Any, *, _job_id: str = job_id) -> None:
            self._decrement_running(_job_id)

        task.future.add_done_callback(_on_complete)
        logger.debug(
            "Dispatching scheduled job '%s' (next run: %s)",
            job_id,
            job.next_run.isoformat(),
        )
        return True

    def _decrement_running(self, job_id: str) -> None:
        """🧮 Reduce the running counter once a job completes."""

        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job.running = max(0, job.running - 1)

        self._wake_event.set()

    def _wait(self, context: ServiceContext, seconds: float) -> bool:
        """⏳ Sleep cooperatively until the next job needs dispatching."""

        if seconds <= 0:
            return not context.should_stop

        deadline = monotonic() + seconds
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                break

            slice_length = min(remaining, 0.5)
            if context.stop_event.wait(timeout=slice_length):
                return False

            if self._wake_event.is_set():
                self._wake_event.clear()
                return not context.should_stop

        if self._wake_event.is_set():
            self._wake_event.clear()
        return not context.should_stop

    def _now_for_job(self, job: ScheduledJob) -> datetime:
        """🕰 Resolve the current time for the job's timezone."""

        tz = job.next_run.tzinfo
        return datetime.now(tz=tz) if tz else datetime.now()


__all__ = [
    "SchedulerCallable",
    "SchedulerService",
    "ScheduledJob",
]
