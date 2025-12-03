"""🎛️ Public scheduler API built on top of the scheduler service."""

from __future__ import annotations

from datetime import datetime, time, timedelta, tzinfo
from functools import wraps
from inspect import iscoroutinefunction
from pathlib import Path
from time import sleep
from typing import Any, Callable, Iterable, Optional, cast

from loguru import logger

from scriptman.powers.generics import P, R

from .host import SchedulerHost
from .models import Job
from .service import SchedulerService
from .triggers import (
    IntervalTrigger,
    OneTimeTrigger,
    SchedulerTrigger,
    TimeOfDayTrigger,
)


def _await_async(awaitable: Any) -> Any:
    from scriptman.powers.tasks import TaskManager

    return TaskManager.await_async(awaitable)


class TaskScheduler:
    """🗓️ High level scheduler facade backed by the TaskManager."""

    def __init__(self, host: SchedulerHost, service: SchedulerService) -> None:
        from scriptman.core._scripts import Scripts
        from scriptman.core._summary import JobSummaryService

        self._host = host
        self._service: SchedulerService | None = service
        self._scripts = Scripts()
        self._summary = JobSummaryService()
        self._jobs: dict[str, Job] = {}

    # ------------------------------------------------------------------
    # Job registration
    # ------------------------------------------------------------------

    def add_job(self, job: Job) -> None:
        """➕ Register a job with the scheduler service.

        Args:
            job: Fully prepared :class:`Job` definition to register.
        """

        self._register_job(job, store=True)

    def remove_job(self, job_id: str) -> bool:
        """➖ Remove a job from the scheduler service.

        Args:
            job_id: Identifier of the job to remove.

        Returns:
            ``True`` if a job was removed, ``False`` otherwise.
        """

        service = self._service_required()
        removed = service.remove_job(job_id)
        self._jobs.pop(job_id, None)
        if removed:
            logger.info(f"➖ Removed scheduled job: {job_id}")
        else:
            logger.warning(f"Job with ID {job_id} not found")
        return removed

    def pause_job(self, job_id: str) -> None:
        """⏸ Pause a job.

        Args:
            job_id: Identifier of the job to pause.
        """

        job = self._jobs.get(job_id)
        if not job:
            logger.warning(f"Job with ID {job_id} not found")
            return
        if self._service_required().set_job_enabled(job_id, False):
            job.enabled = False
            logger.info(f"⏸ Paused scheduled job: {job_id}")

    def resume_job(self, job_id: str) -> None:
        """▶️ Resume a job.

        Args:
            job_id: Identifier of the job to resume.
        """

        job = self._jobs.get(job_id)
        if not job:
            logger.warning(f"Job with ID {job_id} not found")
            return
        if self._service_required().set_job_enabled(job_id, True):
            job.enabled = True
            logger.info(f"▶️ Resumed scheduled job: {job_id}")

    def change_job_trigger(self, job_id: str, trigger: SchedulerTrigger) -> None:
        """🔄 Change the trigger for a job.

        Args:
            job_id: Identifier of the job to modify.
            trigger: New trigger definition.
        """

        self._ensure_trigger_supported(trigger)
        job = self._jobs.get(job_id)
        if not job:
            logger.warning(f"Job with ID {job_id} not found")
            return

        if self._service_required().update_job_trigger(job_id, trigger):
            job.trigger = trigger
            logger.info(f"🔄 Updated trigger for job: {job_id}")

    # ------------------------------------------------------------------
    # Script helpers
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
        """📄 Schedule a script to run using the scheduler.

        Args:
            script_path: Path to the script file to execute.
            job_id: Identifier for the scheduled job.
            trigger: Trigger controlling the run cadence.
            name: Optional display name for logs.
            enabled: Whether to start the job immediately.
            max_instances: Maximum concurrent executions allowed.
        """

        path = self._ensure_script_path(script_path)
        job_name = name or f"script_{path.stem}"

        def execute_script() -> None:
            logger.info(f"▶️ Executing scheduled script: {path}")
            task = self._host.background(self._scripts.run_scripts, [path])
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

    def schedule_script_once(
        self,
        script_path: Path | str,
        job_id: str,
        *,
        run_at: datetime,
        name: Optional[str] = None,
        enabled: bool = True,
    ) -> None:
        """🎯 Schedule a script to run exactly once at ``run_at``.

        Args:
            script_path: Path to the script file to execute.
            job_id: Identifier for the scheduled job.
            run_at: Exact datetime for the one-off execution.
            name: Optional display name for logs.
            enabled: Whether to activate the job immediately.
        """

        self.schedule_script(
            script_path,
            job_id,
            trigger=OneTimeTrigger(run_at=run_at),
            name=name,
            enabled=enabled,
        )

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
        """🪟 Schedule a script at an interval within a time window.

        Args:
            script_path: Path to the script file to execute.
            job_id: Identifier for the scheduled job.
            interval_minutes: Minutes between executions while in the window.
            start_time: Earliest time-of-day to run.
            end_time: Latest time-of-day to run.
            name: Optional display name for logs.
            enabled: Whether to activate immediately.
            max_instances: Maximum concurrent executions.
            timezone: Optional timezone for window evaluation.
        """

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
            task = self._host.background(self._scripts.run_scripts, [path])
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
    # Function helpers
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
        """📞 Schedule a callable to run using the scheduler.

        Args:
            func: Callable to execute.
            job_id: Identifier for the scheduled job.
            trigger: Trigger controlling the run cadence.
            name: Optional display name for logs.
            enabled: Whether to activate immediately.
            max_instances: Maximum concurrent executions.
            args: Positional arguments passed to ``func``.
            kwargs: Keyword arguments passed to ``func``.
        """

        args_tuple = tuple(args or ())
        kwargs_dict = dict(kwargs or {})
        job_name = name or f"function_{func.__name__}"
        is_async = iscoroutinefunction(func)

        def invoke() -> None:
            logger.info(f"▶️ Executing scheduled function: {func.__name__}")
            if is_async:
                _await_async(func(*args_tuple, **kwargs_dict))
            else:
                task = self._host.background(func, *args_tuple, **kwargs_dict)
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

    def schedule_function_once(
        self,
        func: Callable[..., Any],
        job_id: str,
        *,
        run_at: datetime,
        name: Optional[str] = None,
        enabled: bool = True,
        args: Optional[Iterable[Any]] = None,
        kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        """🎯 Schedule a callable to run exactly once.

        Args:
            func: Callable to execute.
            job_id: Identifier for the scheduled job.
            run_at: Exact datetime for the one-off execution.
            name: Optional display name for logs.
            enabled: Whether to activate immediately.
            args: Positional arguments passed to ``func``.
            kwargs: Keyword arguments passed to ``func``.
        """

        self.schedule_function(
            func,
            job_id,
            trigger=OneTimeTrigger(run_at=run_at),
            name=name,
            enabled=enabled,
            args=args,
            kwargs=kwargs,
        )

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
        """🪟 Schedule a callable at an interval within a time window.

        Args:
            func: Callable to execute.
            job_id: Identifier for the scheduled job.
            interval_minutes: Minutes between executions while in the window.
            start_time: Earliest time-of-day to run.
            end_time: Latest time-of-day to run.
            name: Optional display name for logs.
            enabled: Whether to activate immediately.
            max_instances: Maximum concurrent executions.
            args: Positional arguments passed to ``func``.
            kwargs: Keyword arguments passed to ``func``.
            timezone: Optional timezone for window evaluation.
        """

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
                task = self._host.background(func, *args_tuple, **kwargs_dict)
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
        """🎀 Decorator form of :meth:`schedule_function`.

        Args:
            trigger: Trigger controlling the run cadence.
            job_id: Optional explicit job identifier. Defaults to function name.
            name: Optional display name for logs.
            enabled: Whether to activate immediately.
            max_instances: Maximum concurrent executions.
            time_window: Optional pair of times restricting execution window.
            timezone: Optional timezone for window evaluation.

        Returns:
            The decorated callable.
        """

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
    # Lifecycle helpers
    # ------------------------------------------------------------------

    def start_service(self, *, block: bool = True) -> None:
        """▶️ Start the underlying scheduler service loop.

        Args:
            block: When ``True`` this call blocks until the service stops.
        """

        self._host.start_service(SchedulerService._SERVICE_NAME)
        if not block:
            return

        try:
            while self._host.has_running_services():
                sleep(1)
        except KeyboardInterrupt:
            logger.info("Received exit signal")
        finally:
            self.stop_service()

    def stop_service(self) -> None:
        """⏹ Stop the underlying scheduler service loop."""

        self._host.stop_service(SchedulerService._SERVICE_NAME, timeout=5)

    def fast_api_startup_handler(self) -> None:
        """🚀 FastAPI startup hook to start the scheduler service."""

        self.start_service(block=False)

    def fast_api_shutdown_handler(self) -> None:
        """🛑 FastAPI shutdown hook to stop the scheduler service."""

        self.stop_service()

    def list_jobs(self) -> list[dict[str, Any]]:
        """🗓️ List all scheduled jobs.

        Returns:
            A list of dictionaries describing each job's status.
        """

        runtime_jobs = {job.id: job for job in self._service_required().list_jobs()}
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

    def rebind_service(self, service: Optional[SchedulerService]) -> None:
        """🔁 Rebind to a freshly created scheduler service after restart.

        Args:
            service: Newly created service instance or ``None`` when shutting down.
        """

        self._service = service
        if service is None:
            return
        for job in list(self._jobs.values()):
            try:
                self._register_job(job, store=False)
            except ValueError:
                continue

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_trigger_supported(self, trigger: SchedulerTrigger) -> None:
        if not isinstance(trigger, (IntervalTrigger, TimeOfDayTrigger, OneTimeTrigger)):
            raise TypeError(
                "Unsupported trigger type. Only IntervalTrigger, TimeOfDayTrigger, "
                "and OneTimeTrigger are supported."
            )

    @staticmethod
    def _ensure_script_path(path: Path | str) -> Path:
        """🗂 Validate and resolve the script path."""

        resolved = Path(path)
        if not resolved.exists():
            raise FileNotFoundError(f"Script not found: {resolved}")
        return resolved

    def _service_required(self) -> SchedulerService:
        """🧷 Fetch the bound service or raise if unavailable."""

        if self._service is None:
            raise RuntimeError("Scheduler service is not available")
        return self._service

    def _register_job(self, job: Job, *, store: bool) -> None:
        """📝 Register a job with the scheduler service and local cache."""

        service = self._service_required()
        self._ensure_trigger_supported(job.trigger)

        if store and job.id in self._jobs:
            raise ValueError(f"Job '{job.id}' is already registered")

        runner = self._create_runner(job)

        if isinstance(job.trigger, IntervalTrigger):
            service.add_interval_job(
                runner,
                job_id=job.id,
                every=job.trigger.interval,
                max_instances=job.max_instances,
                enabled=job.enabled,
            )
        elif isinstance(job.trigger, TimeOfDayTrigger):
            service.add_time_of_day_job(
                runner,
                job_id=job.id,
                at=job.trigger.at,
                timezone=job.trigger.timezone,
                max_instances=job.max_instances,
                enabled=job.enabled,
            )
        elif isinstance(job.trigger, OneTimeTrigger):
            service.add_one_time_job(
                runner,
                job_id=job.id,
                run_at=job.trigger.run_at,
                enabled=job.enabled,
            )

        if store:
            self._jobs[job.id] = job
        logger.info(f"➕ Added scheduled job: {job.name} (id={job.id})")

    def _create_runner(self, job: Job) -> Callable[[], None]:
        """🏃‍♂️ Wrap the job callable with summary tracking."""

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


__all__ = ["TaskScheduler"]
