"""
⚡ High-level scheduling API built on top of ``SchedulerService``.

This module provides a high-level scheduling API that allows components to
schedule jobs to run at specific times or intervals. The scheduling API is
built on top of the ``SchedulerService``, which allows it to integrate seamlessly
with the task manager's lifecycle management.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, tzinfo
from functools import wraps
from inspect import iscoroutinefunction
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional, cast

from loguru import logger

from scriptman.core._scripts import Scripts
from scriptman.core._summary import JobSummaryService
from scriptman.powers.generics import Func, P, R
from scriptman.powers.tasks._scheduler_service import (
    IntervalTrigger,
    SchedulerService,
    SchedulerTrigger,
    TimeOfDayTrigger,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from scriptman.powers.tasks import TaskManager


def _await_async(awaitable: Any) -> Any:
    """Run an awaitable to completion using TaskManager's helper."""

    from scriptman.powers.tasks import TaskManager

    return TaskManager.await_async(awaitable)


@dataclass(slots=True)
class Job:
    """Representation of a scheduled job."""

    id: str
    name: str
    func: Func[..., Any]
    trigger: SchedulerTrigger
    enabled: bool = True
    max_instances: int = 1
    start_time: time | None = None
    end_time: time | None = None
    time_zone: tzinfo | None = None

    def __post_init__(self) -> None:
        if not self.id or self.id.isspace():
            raise ValueError("Job id cannot be empty")
        if not self.name or self.name.isspace():
            raise ValueError("Job name cannot be empty")
        if self.max_instances < 1:
            raise ValueError("max_instances must be at least 1")
        if self.start_time and self.end_time and self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")


class TaskScheduler:
    """Public scheduling facade exposed via ``TaskManager.scheduler``."""

    def __init__(self, task_manager: "TaskManager") -> None:
        # Local import avoids circular dependency during module import time.
        from scriptman.powers.tasks import TaskManager

        if not isinstance(task_manager, TaskManager):
            raise TypeError("TaskScheduler requires a TaskManager instance")

        self._task_manager = task_manager
        self._service: SchedulerService = task_manager._get_scheduler_service()
        self._scripts = Scripts()
        self._summary = JobSummaryService()
        self._jobs: dict[str, Job] = {}

    # ------------------------------------------------------------------
    # Job registration
    # ------------------------------------------------------------------

    def add_job(self, job: Job) -> None:
        """Register a job with the scheduler service."""

        self._register_job(job, store=True)

    def remove_job(self, job_id: str) -> bool:
        removed = self._service.remove_job(job_id)
        self._jobs.pop(job_id, None)
        if removed:
            logger.info(f"➖ Removed scheduled job: {job_id}")
        else:
            logger.warning(f"Job with ID {job_id} not found")
        return removed

    def pause_job(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if not job:
            logger.warning(f"Job with ID {job_id} not found")
            return
        if self._service.set_job_enabled(job_id, False):
            job.enabled = False
            logger.info(f"⏸ Paused scheduled job: {job_id}")

    def resume_job(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if not job:
            logger.warning(f"Job with ID {job_id} not found")
            return
        if self._service.set_job_enabled(job_id, True):
            job.enabled = True
            logger.info(f"▶️ Resumed scheduled job: {job_id}")

    def change_job_trigger(self, job_id: str, trigger: SchedulerTrigger) -> None:
        self._ensure_trigger_supported(trigger)
        job = self._jobs.get(job_id)
        if not job:
            logger.warning(f"Job with ID {job_id} not found")
            return

        if self._service.update_job_trigger(job_id, trigger):
            job.trigger = trigger
            logger.info(f"🔄 Updated trigger for job: {job_id}")

    # ------------------------------------------------------------------
    # Script scheduling helpers
    # ------------------------------------------------------------------

    def schedule_script(
        self,
        script_path: Path | str,
        job_id: str,
        trigger: SchedulerTrigger,
        *,
        name: Optional[str] = None,
        enabled: bool = True,
        max_instances: int = 1,
    ) -> None:
        path = self._ensure_script_path(script_path)
        job_name = name or f"script_{path.stem}"

        def execute_script() -> None:
            logger.info(f"▶️ Executing scheduled script: {path}")
            task = self._task_manager.background(self._scripts.run_scripts, [path])
            task.await_result()

        job = Job(
            id=job_id,
            name=job_name,
            func=execute_script,
            trigger=trigger,
            enabled=enabled,
            max_instances=max_instances,
        )
        self.add_job(job)
        logger.info(f"📅 Scheduled script {path} with job ID: {job_id}")

    def schedule_script_in_time_window(
        self,
        script_path: Path | str,
        job_id: str,
        interval_minutes: int,
        start_time: time,
        end_time: time,
        *,
        name: Optional[str] = None,
        enabled: bool = True,
        max_instances: int = 1,
        timezone: Optional[tzinfo] = None,
    ) -> None:
        if interval_minutes < 1:
            raise ValueError("Interval must be at least 1 minute")
        if end_time <= start_time:
            raise ValueError("end_time must be after start_time")

        path = self._ensure_script_path(script_path)
        job_name = name or path.stem
        trigger = IntervalTrigger(timedelta(minutes=interval_minutes))

        def execute_script() -> None:
            current_time = datetime.now(timezone).time()
            if current_time < start_time:
                logger.info(f"⏳ Waiting for {start_time} for job {job_name}")
                return
            if current_time > end_time:
                logger.info(f"⏹️ Job {job_name} has reached end time {end_time}")
                return

            logger.info(f"▶️ Executing scheduled script: {path}")
            task = self._task_manager.background(self._scripts.run_scripts, [path])
            task.await_result()

        job = Job(
            id=job_id,
            name=job_name,
            func=execute_script,
            trigger=trigger,
            enabled=enabled,
            max_instances=max_instances,
            start_time=start_time,
            end_time=end_time,
            time_zone=timezone,
        )
        self.add_job(job)
        logger.info(
            f"🕒 Scheduled script {path} to run every {interval_minutes} minutes "
            f"between {start_time} and {end_time} with job ID: {job_id}"
        )

    # ------------------------------------------------------------------
    # Function scheduling helpers
    # ------------------------------------------------------------------

    def schedule_function(
        self,
        func: Callable[..., Any],
        job_id: str,
        trigger: SchedulerTrigger,
        *,
        name: Optional[str] = None,
        enabled: bool = True,
        max_instances: int = 1,
        args: Optional[Iterable[Any]] = None,
        kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        args_tuple = tuple(args or ())
        kwargs_dict = dict(kwargs or {})
        job_name = name or f"function_{func.__name__}"
        is_async = iscoroutinefunction(func)

        def invoke() -> None:
            logger.info(f"▶️ Executing scheduled function: {func.__name__}")
            if is_async:
                _await_async(func(*args_tuple, **kwargs_dict))
            else:
                task = self._task_manager.background(func, *args_tuple, **kwargs_dict)
                task.await_result()

        job = Job(
            id=job_id,
            name=job_name,
            func=invoke,
            trigger=trigger,
            enabled=enabled,
            max_instances=max_instances,
        )
        self.add_job(job)
        logger.info(f"📅 Scheduled function {func.__name__} with job ID: {job_id}")

    def schedule_function_in_time_window(
        self,
        func: Callable[..., Any],
        job_id: str,
        interval_minutes: int,
        start_time: time,
        end_time: time,
        *,
        name: Optional[str] = None,
        enabled: bool = True,
        max_instances: int = 1,
        args: Optional[Iterable[Any]] = None,
        kwargs: Optional[dict[str, Any]] = None,
        timezone: Optional[tzinfo] = None,
    ) -> None:
        if interval_minutes < 1:
            raise ValueError("Interval must be at least 1 minute")
        if end_time <= start_time:
            raise ValueError("end_time must be after start_time")

        args_tuple = tuple(args or ())
        kwargs_dict = dict(kwargs or {})
        job_name = name or func.__name__
        trigger = IntervalTrigger(timedelta(minutes=interval_minutes))
        is_async = iscoroutinefunction(func)

        def invoke() -> None:
            current_time = datetime.now(timezone).time()
            if current_time < start_time:
                logger.info(f"⏳ Waiting for {start_time} for job {job_name}")
                return
            if current_time > end_time:
                logger.info(f"⏹️ Job {job_name} has reached end time {end_time}")
                return

            logger.info(f"▶️ Executing scheduled function: {func.__name__}")
            if is_async:
                _await_async(func(*args_tuple, **kwargs_dict))
            else:
                task = self._task_manager.background(func, *args_tuple, **kwargs_dict)
                task.await_result()

        job = Job(
            id=job_id,
            name=job_name,
            func=invoke,
            trigger=trigger,
            enabled=enabled,
            max_instances=max_instances,
            start_time=start_time,
            end_time=end_time,
            time_zone=timezone,
        )
        self.add_job(job)
        logger.info(
            f"🕒 Scheduled job {job.name} to run every {interval_minutes} minutes "
            f"between {start_time} and {end_time} with job ID: {job_id}"
        )

    # ------------------------------------------------------------------
    # Decorator API
    # ------------------------------------------------------------------

    def schedule(
        self,
        trigger: SchedulerTrigger,
        *,
        job_id: Optional[str] = None,
        name: Optional[str] = None,
        enabled: bool = True,
        max_instances: int = 1,
        time_window: Optional[tuple[time, time]] = None,
        timezone: Optional[tzinfo] = None,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            job_identifier = job_id or f"{func.__name__}_job"
            display_name = name or func.__name__.replace("_", " ").title()

            @wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                if iscoroutinefunction(func):
                    logger.debug("🔄 Executing async scheduled function")
                    result: R = cast(R, _await_async(func(*args, **kwargs)))
                    return result
                logger.debug("🔄 Executing sync scheduled function")
                return func(*args, **kwargs)

            if time_window:
                start_time, end_time = time_window
                if not isinstance(trigger, IntervalTrigger):
                    raise TypeError("time_window requires an IntervalTrigger trigger")
                seconds = trigger.interval.total_seconds()
                interval_minutes = max(1, int(seconds // 60) or 1)
                self.schedule_function_in_time_window(
                    func=wrapper,
                    job_id=job_identifier,
                    interval_minutes=interval_minutes,
                    start_time=start_time,
                    end_time=end_time,
                    name=display_name,
                    enabled=enabled,
                    max_instances=max_instances,
                    timezone=timezone,
                )
            else:
                self.schedule_function(
                    func=wrapper,
                    job_id=job_identifier,
                    trigger=trigger,
                    name=display_name,
                    enabled=enabled,
                    max_instances=max_instances,
                )

            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # Service lifecycle helpers
    # ------------------------------------------------------------------

    def start_service(self, *, block: bool = True) -> None:
        self._task_manager.start_service(SchedulerService._SERVICE_NAME)
        if not block:
            return

        try:
            while self._task_manager.has_running_services():
                sleep(1)
        except KeyboardInterrupt:
            logger.info("Received exit signal")
        finally:
            self.stop_service()

    def stop_service(self) -> None:
        self._task_manager.stop_service(SchedulerService._SERVICE_NAME, timeout=5)

    def fast_api_startup_handler(self) -> None:
        self.start_service(block=False)

    def fast_api_shutdown_handler(self) -> None:
        self.stop_service()

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def list_jobs(self) -> list[dict[str, Any]]:
        runtime_jobs = {job.id: job for job in self._service.list_jobs()}
        entries: list[dict[str, Any]] = []
        for job_id, job in self._jobs.items():
            runtime = runtime_jobs.get(job_id)
            entries.append(
                {
                    "id": job_id,
                    "name": job.name,
                    "enabled": runtime.enabled if runtime else job.enabled,
                    "next_run": runtime.next_run.isoformat() if runtime else None,
                    "max_instances": job.max_instances,
                    "running_instances": runtime.running if runtime else 0,
                }
            )
        return entries

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_runner(self, job: Job) -> Callable[[], None]:
        func = job.func
        job_id = job.id
        job_name = job.name

        def runner() -> None:
            try:
                logger.info(f"▶️ Executing scheduled job: {job_name}")
                func()
                self._summary.add_job(job_id, job_name, success=True)
                logger.success(f"✅ Job {job_name} executed successfully")
            except Exception as exc:  # noqa: BLE001
                self._summary.add_job(job_id, job_name, success=False, error=exc)
                logger.error(f"❌ Job {job_name} failed: {exc}")
                raise

        return runner

    def _ensure_trigger_supported(self, trigger: SchedulerTrigger) -> None:
        if not isinstance(trigger, (IntervalTrigger, TimeOfDayTrigger)):
            raise TypeError(
                "Unsupported trigger type. Only IntervalTrigger and "
                "TimeOfDayTrigger are supported."
            )

    @staticmethod
    def _ensure_script_path(path: Path | str) -> Path:
        resolved = Path(path)
        if not resolved.exists():
            raise FileNotFoundError(f"Script not found: {resolved}")
        return resolved

    def _register_job(self, job: Job, *, store: bool) -> None:
        self._ensure_trigger_supported(job.trigger)

        if store and job.id in self._jobs:
            raise ValueError(f"Job '{job.id}' is already registered")

        runner = self._create_runner(job)

        if isinstance(job.trigger, IntervalTrigger):
            self._service.add_interval_job(
                runner,
                job_id=job.id,
                every=job.trigger.interval,
                max_instances=job.max_instances,
                enabled=job.enabled,
            )
        elif isinstance(job.trigger, TimeOfDayTrigger):
            self._service.add_time_of_day_job(
                runner,
                job_id=job.id,
                at=job.trigger.time_of_day,
                timezone=job.trigger.timezone,
                max_instances=job.max_instances,
                enabled=job.enabled,
            )

        if store:
            self._jobs[job.id] = job
        logger.info(f"➕ Added scheduled job: {job.name} (id={job.id})")

    def on_manager_restart(self) -> None:
        self._service = self._task_manager._get_scheduler_service()
        for job in list(self._jobs.values()):
            try:
                self._register_job(job, store=False)
            except ValueError:
                continue


__all__ = ["Job", "TaskScheduler"]
