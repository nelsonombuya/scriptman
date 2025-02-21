try:
    from typing import Any, Generic, Optional

    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.schedulers.base import BaseScheduler
    from loguru import logger

    from scriptman.core.config import Config
    from scriptman.powers.generics import T
    from scriptman.powers.scheduler._models import Job

except ImportError:
    raise ImportError(
        "APScheduler is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[schedule]."
    )


class Scheduler(Generic[T]):
    __initialized: bool = False
    __instance: Optional["Scheduler[T]"] = None
    __scheduler: BaseScheduler = AsyncIOScheduler()

    def __new__(cls, *args: Any, **kwargs: Any) -> "Scheduler[T]":
        """
        1️⃣ Ensure only a single instance of Scheduler exists (singleton pattern).

        This method checks if an instance of Scheduler already exists.
        If not, it creates a new instance, sets the __initialized flag to False,
        and returns the instance.
        """
        if cls.__instance is None:
            cls.__instance = super(Scheduler, cls).__new__(cls, *args, **kwargs)
            cls.__instance.__initialized = False
        return cls.__instance

    def __init__(self) -> None:
        """
        🚀 Initialize the Scheduler singleton instance.

        This method starts the scheduler and sets the __initialized flag to True.
        If the instance has already been initialized, it simply returns.
        """
        if self.__initialized:
            return

        self.__scheduler.start()
        self.__initialized = True

    def add_job(self, job: Job[T]) -> None:
        """
        ➕ Add a job to the scheduler.

        Args:
            job: A Job instance to be added to the scheduler.

        Notes:
            If the job is enabled, it is added to the scheduler.
            If the job is disabled, a warning is logged.
        """
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
        if job_id in self.__scheduler.get_jobs():
            logger.info(f"➖ Removing scheduled job: {job_id}")
            self.__scheduler.remove_job(job_id)
        else:
            logger.warning(f"Job with ID {job_id} not found")

    def __del__(self) -> None:
        """
        👋 Goodbye! Shut down the scheduler when the instance is deleted.
        """
        logger.info("👋 Goodbye! Shutting down the scheduler...")
        self.__scheduler.shutdown()


# Adding the Scheduler singleton instance to the Config class
object.__setattr__(Config, "scheduler", Scheduler())
__all__: list[str] = ["Scheduler", "Job"]
