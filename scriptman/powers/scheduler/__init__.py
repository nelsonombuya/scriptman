try:
    from pathlib import Path
    from threading import RLock
    from typing import Any, Callable, Optional

    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.schedulers.base import BaseScheduler
    from apscheduler.triggers.base import BaseTrigger
    from loguru import logger

    from scriptman.core._scripts import Scripts
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
    __instance: Optional["Scheduler"] = None
    __scheduler: BaseScheduler = AsyncIOScheduler()
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
        thread_pool_size: Optional[int] = None,
        process_pool_size: Optional[int] = None,
    ) -> None:
        """
        🚀 Initialize the Scheduler singleton instance.

        This method starts the scheduler and sets the __initialized flag to True.
        If the instance has already been initialized, it simply returns.

        Args:
            thread_pool_size: The number of threads to use for the task executor.
            process_pool_size: The number of processes to use for the task executor.
        """
        with self.__lock:
            if self.__initialized:
                return

            self.__scheduler.start()

            # Script & Function Scheduling
            self.__scripts = Scripts()
            self.__task_executor = TaskExecutor(
                thread_pool_size=thread_pool_size,
                process_pool_size=process_pool_size,
            )

            self.__initialized = True

    def add_job(self, job: Job) -> None:
        """
        ➕ Add a job to the scheduler.

        Args:
            job: A Job instance to be added to the scheduler.

        Notes:
            If the job is enabled, it is added to the scheduler.
            If the job is disabled, a warning is logged.
        """
        with self.__lock:
            if job.enabled:
                logger.info(f"➕ Adding scheduled job: {job.name}")
                self.__scheduler.add_job(**job.model_dump())
            else:
                logger.warning(f"Job {job.name} is disabled")

    def remove_job(self, job_id: str) -> None:
        """
        ➖ Remove a scheduled job by its ID.

        Args:
            job_id: The ID of the job to be removed.

        Notes:
            If the job is found, it is removed from the scheduler.
            If the job is not found, a warning is logged.
        """
        with self.__lock:
            if job_id in self.__scheduler.get_jobs():
                logger.info(f"➖ Removing scheduled job: {job_id}")
                self.__scheduler.remove_job(job_id)
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

    def change_scheduler(self, scheduler: BaseScheduler) -> None:
        """
        🔄 Change the scheduler.

        Args:
            scheduler: The new scheduler.

        Notes:
            If the scheduler is not found, a warning is logged.
        """
        with self.__lock:
            logger.info(f"🔄 Changing scheduler to: {scheduler}")
            self.__scheduler = scheduler

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
            task = self.__task_executor.background(
                self.__scripts.run_scripts, [script_path]
            )
            task.await_result()  # Wait for completion to catch any errors

        # Create and add the job
        job = Job(
            id=job_id,
            name=job_name,
            function=execute_script,
            trigger=trigger,
            enabled=enabled,
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
            task = self.__task_executor.background(func, *args, **kwargs)
            task.await_result()  # Wait for completion to catch any errors

        # Create and add the job
        job = Job(
            id=job_id,
            name=job_name,
            function=execute_function,
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
                    function=job.func,
                    trigger=job.trigger,
                    enabled=job.next_run_time is not None,
                    max_instances=job.max_instances,
                ).info_dump()
                for job in self.__scheduler.get_jobs()
            ]

    def __del__(self) -> None:
        """
        👋 Goodbye! Shut down the scheduler when the instance is deleted.
        """
        with self.__lock:
            logger.info("👋 Goodbye! Shutting down the scheduler...")
            self.__scheduler.shutdown()


# Creating the singleton instance
scheduler: Scheduler = Scheduler()
__all__: list[str] = ["Scheduler", "Job", "scheduler"]
