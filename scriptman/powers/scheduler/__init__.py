try:
    from threading import RLock
    from typing import Any, Optional

    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.schedulers.base import BaseScheduler
    from apscheduler.triggers.base import BaseTrigger
    from loguru import logger

    from scriptman.powers.scheduler._models import Job

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

    def __init__(self) -> None:
        """
        🚀 Initialize the Scheduler singleton instance.

        This method starts the scheduler and sets the __initialized flag to True.
        If the instance has already been initialized, it simply returns.
        """
        with self.__lock:
            if self.__initialized:
                return

            self.__scheduler.start()
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
            logger.info(f"�� Changing scheduler to: {scheduler}")
            self.__scheduler = scheduler

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
