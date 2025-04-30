try:
    from datetime import datetime
    from pathlib import Path
    from threading import RLock, Thread
    from typing import Any, Callable, Optional

    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.schedulers.base import BaseScheduler
    from apscheduler.triggers.base import BaseTrigger
    from apscheduler.triggers.cron import CronTrigger
    from loguru import logger

    from scriptman.core._scripts import Scripts
    from scriptman.core._summary import JobSummaryService
    from scriptman.powers.scheduler._models import Job
    from scriptman.powers.task import TaskExecutor

except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[schedule]."
    )


class Scheduler:
    __initialized: bool = False
    __is_service_running: bool = False
    __instance: Optional["Scheduler"] = None
    __service_thread: Optional[Thread] = None
    __lock: RLock = RLock()  # Reentrant lock for thread safety

    def __new__(cls, *args: Any, **kwargs: Any) -> "Scheduler":
        """
        1️⃣ Ensure only a single instance of Scheduler exists (singleton pattern).

        This method checks if an instance of Scheduler already exists in a thread-safe
        manner.

        If not, it creates a new instance, sets the __initialized flag to False,
        and returns the instance.
        """
        with cls.__lock:
            if cls.__instance is None:
                cls.__instance = super(Scheduler, cls).__new__(cls, *args, **kwargs)
                cls.__instance.__initialized = False
        return cls.__instance

    def __init__(
        self,
        scheduler_class: type[BaseScheduler] = BackgroundScheduler,
        thread_pool_size: int | None = None,
        process_pool_size: int | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        """
        🚀 Initialize the scheduler singleton with flexibility to change scheduler type.

        Args:
            scheduler_class: The scheduler class to use (default: BackgroundScheduler)
            thread_pool_size: The number of threads to use for the task executor.
            process_pool_size: The number of processes to use for the task executor.
            **kwargs: Additional arguments to pass to the scheduler constructor
        """
        with self.__lock:
            if self.__initialized:
                return

            # Scheduler Setup
            self.__scheduler = scheduler_class(**kwargs)
            self.__scheduler.start()
            self.__service_thread = None
            self.__is_service_running = False

            # Script & Function Scheduling
            self.__scripts = Scripts()
            self.__summary = JobSummaryService()
            self.__executor = TaskExecutor(
                thread_pool_size=thread_pool_size,
                process_pool_size=process_pool_size,
            )

            # Schedule the daily summary release job
            self._schedule_daily_summary_release()

            # Initialize the scheduler instance
            self.__initialized = True

    def _schedule_daily_summary_release(self) -> None:
        """📊 Schedule a job to release the daily job summary at 11 PM."""

        def release_daily_summary() -> None:
            """Release the daily job summary."""
            try:
                today = datetime.now().date().isoformat()
                summary = self.__summary.get_summary(today)
                if summary:
                    logger.info(f"📊 Releasing daily job summary for {today}")
                    logger.info(f"Total jobs: {summary['total_jobs']}")
                    logger.info(f"Successful jobs: {summary['successful_jobs']}")
                    logger.info(f"Failed jobs: {summary['failed_jobs']}")

                    # Clean up old summaries
                    self.__summary.cleanup_old_summaries()
            except Exception as e:
                logger.error(f"❌ Error releasing daily summary: {e}")

        # Schedule the job to run at 11 PM every day
        trigger = CronTrigger(hour=23, minute=0)
        job = Job(
            id="daily_summary_release",
            name="Daily Job Summary Release",
            func=release_daily_summary,
            trigger=trigger,
            enabled=True,
            max_instances=1,
        )
        self.add_job(job)
        logger.info("📅 Scheduled daily job summary release at 11 PM")

    @property
    def scheduler(self) -> BaseScheduler:
        """Get the underlying APScheduler instance for advanced operations"""
        return self.__scheduler

    def add_job(self, job: Job) -> None:
        """➕ Add a job to the scheduler."""
        with self.__lock:
            if job.enabled:
                logger.info(f"➕ Adding scheduled job: {job.name}")
                self.__scheduler.add_job(**job.model_dump())
            else:
                logger.warning(f"Job {job.name} is disabled")

    def remove_job(self, job_id: str) -> None:
        """➖ Remove a scheduled job by its ID."""
        with self.__lock:
            if job_id in self.__scheduler.get_jobs():
                logger.info(f"➖ Removing scheduled job: {job_id}")
                self.__scheduler.remove_job(job_id)
            else:
                logger.warning(f"Job with ID {job_id} not found")

    def pause_job(self, job_id: str) -> None:
        """⏸ Pause a scheduled job by its ID"""
        with self.__lock:
            if job_id in self.__scheduler.get_jobs():
                logger.info(f"⏸ Pausing scheduled job: {job_id}")
                self.__scheduler.pause_job(job_id)
            else:
                logger.warning(f"Job with ID {job_id} not found")

    def resume_job(self, job_id: str) -> None:
        """▶️ Resume a paused job by its ID"""
        with self.__lock:
            if job_id in self.__scheduler.get_jobs():
                logger.info(f"▶️ Resuming scheduled job: {job_id}")
                self.__scheduler.resume_job(job_id)
            else:
                logger.warning(f"Job with ID {job_id} not found")

    def change_job_trigger(self, job_id: str, trigger: BaseTrigger) -> None:
        """
        🔄 Change the trigger of a scheduled job.

        Args:
            job_id: The ID of the job to be changed.
            trigger: The new trigger for the job.

        Notes:
            If the job is not found, a warning is logged.
        """
        with self.__lock:
            if job_id in self.__scheduler.get_jobs():
                logger.info(f"🔄 Changing trigger for job: {job_id}")
                self.__scheduler.reschedule_job(job_id, trigger)
            else:
                logger.warning(f"Job with ID {job_id} not found")

    def change_scheduler(
        self, scheduler_class: type[BaseScheduler], **kwargs: dict[str, Any]
    ) -> bool:
        """
        🔄 Change the scheduler type and migrate all existing jobs to the new scheduler.

        Args:
            scheduler_class: The new scheduler class to use (e.g., BackgroundScheduler)
            **kwargs: Additional arguments to pass to the new scheduler constructor

        Returns:
            bool: True if migration was successful, False otherwise
        """
        if self.__is_service_running:
            logger.warning(
                "⚠ Cannot change scheduler while service is running. "
                "Stop the service first."
            )
            return False

        current_jobs = self._extract_jobs()
        was_running = self.__scheduler.running

        try:
            if was_running:
                self.__scheduler.shutdown(wait=True)

            self.__scheduler = scheduler_class(**kwargs)

            if was_running:
                self.__scheduler.start()

            self._restore_jobs(current_jobs)
            logger.success(
                f"✅ Successfully migrated {len(current_jobs)} jobs "
                f"to {scheduler_class.__name__}"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Error changing scheduler: {e}")
            # If we fail, try to restore the original scheduler state
            if not self.__scheduler.running and was_running:
                self.__scheduler.start()
            return False

    def _extract_jobs(self) -> list[dict[str, Any]]:
        """📩 Extract all job configurations from the current scheduler."""
        from copy import deepcopy

        return [
            {
                "id": job.id,
                "func": job.func,
                "trigger": deepcopy(job.trigger),
                "name": job.name,
                "misfire_grace_time": job.misfire_grace_time,
                "coalesce": job.coalesce,
                "max_instances": job.max_instances,
                "next_run_time": job.next_run_time,
                # Extract any other job properties you need
            }
            for job in self.__scheduler.get_jobs()
        ]

    def _restore_jobs(self, jobs: list[dict[str, Any]]) -> None:
        """📩 Restore jobs to the current scheduler."""
        for job_details in jobs:
            # The job ID is passed separately
            job_id = job_details.pop("id")

            # Extract the function and trigger which are required parameters
            func = job_details.pop("func")
            trigger = job_details.pop("trigger")

            # Remove next_run_time since APScheduler will calculate this
            if "next_run_time" in job_details:
                job_details.pop("next_run_time")

            # Add the job with all its original parameters
            self.__scheduler.add_job(func=func, trigger=trigger, id=job_id, **job_details)

    def schedule_script(
        self,
        script_path: Path | str,
        job_id: str,
        trigger: BaseTrigger,
        name: Optional[str] = None,
        enabled: bool = True,
        max_instances: int = 1,
    ) -> None:
        """
        📅 Schedule a script for execution

        Args:
            script_path: Path to the Python script
            job_id: Unique identifier for the job
            trigger: APScheduler trigger defining when to run
            name: Human-readable name for the job (defaults to script name)
            enabled: Whether the job should be active
            max_instances: Maximum concurrent instances of this job
        """
        script_path = Path(script_path)
        if not script_path.exists():
            logger.error(f"❌ Script not found: {script_path}")
            return

        job_name = name or f"script_{script_path.stem}"

        # Create a wrapper function that executes the script in a thread
        def execute_script() -> None:
            logger.info(f"▶️ Executing scheduled script: {script_path}")
            try:
                task = self.__executor.background(
                    self.__scripts.run_scripts, [script_path]
                )
                task.await_result()  # Wait for completion to catch any errors
                logger.success(f"✅ Script {script_path} executed successfully")
                self.__summary.add_job(job_id, job_name, success=True)
            except Exception as e:
                logger.error(f"❌ Error executing script {script_path}: {e}")
                self.__summary.add_job(job_id, job_name, success=False, error=e)
                raise  # Re-raise the exception to maintain the original behavior

        # Create and add the job
        job = Job(
            id=job_id,
            name=job_name,
            trigger=trigger,
            enabled=enabled,
            func=execute_script,
            max_instances=max_instances,
        )

        self.add_job(job)
        logger.info(f"📅 Scheduled script {script_path} with job ID: {job_id}")

    def schedule_function(
        self,
        func: Callable[..., Any],
        job_id: str,
        trigger: BaseTrigger,
        name: Optional[str] = None,
        enabled: bool = True,
        max_instances: int = 1,
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        📅 Schedule a function for execution

        Args:
            func: The function to be scheduled
            job_id: Unique identifier for the job
            trigger: APScheduler trigger defining when to run
            name: Human-readable name for the job (defaults to function name)
            enabled: Whether the job should be active
            max_instances: Maximum concurrent instances of this job
            args: Positional arguments to pass to the function
            kwargs: Keyword arguments to pass to the function
        """
        job_name = name or f"function_{func.__name__}"
        args = args or tuple()
        kwargs = kwargs or dict()

        # Create a wrapper function that executes the function in a thread
        def execute_function() -> None:
            logger.info(f"▶️ Executing scheduled function: {func.__name__}")
            try:
                task = self.__executor.background(func, *args, **kwargs)
                task.await_result()  # Wait for completion to catch any errors
                logger.success(f"✅ Function {func.__name__} executed successfully")
                self.__summary.add_job(job_id, job_name, success=True)
            except Exception as e:
                logger.error(f"❌ Error executing function {func.__name__}: {e}")
                self.__summary.add_job(job_id, job_name, success=False, error=e)
                raise  # Re-raise the exception to maintain the original behavior

        # Create and add the job
        job = Job(
            id=job_id,
            name=job_name,
            func=execute_function,
            trigger=trigger,
            enabled=enabled,
            max_instances=max_instances,
        )

        self.add_job(job)
        logger.info(f"📅 Scheduled function {func.__name__} with job ID: {job_id}")

    def list_jobs(self) -> list[dict[str, Any]]:
        """
        📋 List all scheduled jobs and their details

        Returns:
            list[dict[str, Any]]: List of job information dictionaries
        """
        with self.__lock:
            return [
                Job(
                    id=job.id,
                    name=job.name,
                    func=job.func,
                    trigger=job.trigger,
                    enabled=job.next_run_time is not None,
                    max_instances=job.max_instances,
                ).model_dump()
                for job in self.__scheduler.get_jobs()
            ]

    def start_service(self, block: bool = True) -> Optional[Thread]:
        """
        🔄 Run the scheduler as a background service.

        Args:
            block (bool): If True, this function will block the current thread.
                If False, will start in a separate thread and return immediately.

        Returns:
            Optional[Thread]: The service thread if block=False, otherwise None
        """
        if block:
            self._service_loop()
            return None
        else:
            self.__service_thread = Thread(target=self._service_loop, daemon=False)
            self.__service_thread.start()
            return self.__service_thread

    def _service_loop(self) -> None:
        """🔄 Run the scheduler as a background service."""
        self.__is_service_running = True
        logger.info("🔄 Scheduler service started")

        # Set up signal handlers for clean shutdown
        def handle_exit_signal(signum: int, frame: Any) -> None:
            logger.info(f"Received exit signal {signum}")
            self.__is_service_running = False

        try:
            # Register signal handlers (works on Unix-like systems)
            from signal import SIGINT, SIGTERM, signal

            signal(SIGINT, handle_exit_signal)
            signal(SIGTERM, handle_exit_signal)
        except (AttributeError, ValueError):
            # Running in a thread or on Windows where signals work differently
            logger.debug("Service is running in a thread or on Windows")

        try:
            # Keep the service running until requested to stop
            from time import sleep

            while self.__is_service_running:
                sleep(1)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Received exit signal")
        finally:
            logger.info("Scheduler service stopping...")
            self.__scheduler.shutdown()
            logger.info("Scheduler service stopped")

    def stop_service(self) -> bool:
        """🛑 Stop the scheduler service if it's running"""
        if self.__is_service_running:
            self.__is_service_running = False
            if self.__service_thread and self.__service_thread.is_alive():
                self.__service_thread.join(timeout=5)
            return True
        return False

    @property
    def is_service_running(self) -> bool:
        """Check if the scheduler service is running"""
        return self.__is_service_running

    def __del__(self) -> None:
        """👋 Goodbye! Shut down the scheduler when the instance is deleted."""
        try:
            with self.__lock:
                logger.info("👋 Goodbye! Shutting down the scheduler...")
                self.__scheduler.shutdown()
        except Exception as e:
            logger.warning(f"❌ Error shutting down scheduler: {e}")


# Creating the singleton instance
scheduler: Scheduler = Scheduler()
__all__: list[str] = ["Scheduler", "Job", "scheduler"]
