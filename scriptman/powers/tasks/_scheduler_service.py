"""
⚡ Internal scheduler backed by the task service infrastructure.

This module provides a minimal scheduler service that allows components to
schedule jobs to run at specific times or intervals. The scheduler is backed
by the task service infrastructure, which allows it to integrate seamlessly
with the task manager's lifecycle management.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, tzinfo
from threading import Event, RLock
from time import monotonic
from typing import TYPE_CHECKING, Any, Callable, Optional

from loguru import logger

from scriptman.powers.tasks._service_manager import ServiceContext

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
    from scriptman.powers.tasks import TaskManager


SchedulerCallable = Callable[..., Any]


class SchedulerTrigger:
    """Interface for scheduler triggers."""

    def next_run(self, previous: Optional[datetime]) -> datetime:
        raise NotImplementedError  # pragma: no cover - interface method


class IntervalTrigger(SchedulerTrigger):
    """Run jobs on a fixed interval."""

    def __init__(self, interval: timedelta) -> None:
        if interval.total_seconds() <= 0:
            raise ValueError("Interval must be greater than zero")
        self._interval = interval

    def next_run(self, previous: Optional[datetime]) -> datetime:
        base = previous or datetime.now()
        return base + self._interval

    @property
    def interval(self) -> timedelta:
        return self._interval


class TimeOfDayTrigger(SchedulerTrigger):
    """Run jobs once per day at a specific time of day."""

    def __init__(self, *, at: time, timezone: Optional[tzinfo] = None) -> None:
        self._time = at
        self._timezone = timezone

    def next_run(self, previous: Optional[datetime]) -> datetime:
        now = datetime.now(tz=self._timezone)
        candidate = now.replace(
            hour=self._time.hour,
            minute=self._time.minute,
            second=self._time.second,
            microsecond=self._time.microsecond,
        )

        if candidate <= now:
            candidate += timedelta(days=1)
        return candidate

    @property
    def time_of_day(self) -> time:
        return self._time

    @property
    def timezone(self) -> Optional[tzinfo]:
        return self._timezone


@dataclass(slots=True)
class ScheduledJob:
    """Internal job representation for the scheduler service."""

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
    """
    ⚡ Minimal scheduler driven by ``TaskManager`` services.

    This module provides a minimal scheduler service that allows components to
    schedule jobs to run at specific times or intervals. The scheduler is backed
    by the task service infrastructure, which allows it to integrate seamlessly
    with the task manager's lifecycle management.
    """

    _SERVICE_NAME = "task-scheduler-service"

    def __init__(self, task_manager: "TaskManager") -> None:
        from scriptman.powers.tasks import TaskManager  # Local import to avoid circular

        if not isinstance(task_manager, TaskManager):  # pragma: no cover - sanity check
            raise TypeError("SchedulerService requires a TaskManager instance")

        self._task_manager = task_manager
        self._jobs: dict[str, ScheduledJob] = {}
        self._lock = RLock()
        self._wake_event = Event()
        self._service_registered = False

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

    def remove_job(self, job_id: str) -> bool:
        with self._lock:
            removed = self._jobs.pop(job_id, None) is not None
        if removed:
            logger.info(f"Removed scheduled job '{job_id}'")
            self._wake_event.set()
        return removed

    def list_jobs(self) -> list[ScheduledJob]:
        with self._lock:
            return [deepcopy(job) for job in self._jobs.values()]

    def set_job_enabled(self, job_id: str, enabled: bool) -> bool:
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
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False

            job.trigger = trigger
            job.next_run = trigger.next_run(None)
            self._wake_event.set()
            return True

    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        with self._lock:
            job = self._jobs.get(job_id)
            return deepcopy(job) if job else None

    # ------------------------------------------------------------------
    # Internal operations
    # ------------------------------------------------------------------

    def _add_job(self, job: ScheduledJob) -> ScheduledJob:
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
        if self._service_registered:
            return
        try:
            self._task_manager.register_service(
                self._SERVICE_NAME,
                self._service_loop,
                autostart=True,
                daemon=False,
                keep_alive=True,
                restart_delay=1.0,
            )
        except ValueError:
            # Service may already be registered; swallow duplicate registration
            logger.debug("Scheduler service already registered")
        finally:
            self._service_registered = True
            self._wake_event.set()

    def _service_loop(self, context: ServiceContext) -> None:
        logger.info("Scheduler service loop started")

        try:
            while not context.should_stop:
                job_id, wait_time = self._next_job()

                if job_id is None:
                    if not self._wait(context, 1.0):
                        break
                    continue

                if wait_time > 0:
                    if not self._wait(context, wait_time):
                        break

                if context.should_stop:
                    break  # type:ignore[unreachable] # pragma: no cover

                dispatched = self._dispatch(job_id)
                if not dispatched:
                    # We reached max instances; wait a little before retrying
                    if not self._wait(context, 0.5):
                        break

        except Exception as exc:  # noqa: BLE001 - service loops must be resilient
            logger.exception(f"Scheduler service encountered an error: {exc}")
        finally:
            logger.info("Scheduler service loop exiting")

    def _next_job(self) -> tuple[Optional[str], float]:
        with self._lock:
            enabled_jobs = [item for item in self._jobs.items() if item[1].enabled]
            if not enabled_jobs:
                return None, 0.0

            job_id, job = min(enabled_jobs, key=lambda item: item[1].next_run)

        now = self._now_for_job(job)
        wait = max(0.0, (job.next_run - now).total_seconds())
        return job_id, wait

    def _dispatch(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False

            if not job.enabled or job.running >= job.max_instances:
                return False

            scheduled_time = job.next_run
            job.running += 1
            job.next_run = job.trigger.next_run(scheduled_time)
            args = job.args
            kwargs = job.kwargs
            func = job.func

        task = self._task_manager.background(func, *args, **kwargs)

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
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job.running = max(0, job.running - 1)

        self._wake_event.set()

    def _wait(self, context: ServiceContext, seconds: float) -> bool:
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
        tz = job.next_run.tzinfo
        return datetime.now(tz=tz) if tz else datetime.now()


__all__: list[str] = [
    "SchedulerCallable",
    "SchedulerService",
    "ScheduledJob",
    "IntervalTrigger",
    "TimeOfDayTrigger",
    "SchedulerTrigger",
]
