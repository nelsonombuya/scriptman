try:
    from datetime import time, timezone
    from functools import wraps
    from typing import Any

    from apscheduler.triggers.base import BaseTrigger
    from loguru import logger
    from pydantic import BaseModel, ValidationInfo, field_validator
    from pydantic.config import ConfigDict

    from scriptman.core._summary import JobSummaryService
    from scriptman.powers.generics import Func
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[schedule]."
    )


class Job(BaseModel):
    """
    🚀 Represents a job object.

    Attributes:
        id (str): The unique identifier for the job.
        name (str): The name of the job.
        function (AsyncFunc[Any] | SyncFunc[Any]): The function to be executed by the job.
        trigger (BaseTrigger): The trigger object for the job.
        max_instances (int, optional): The maximum number of instances allowed for the
            job. Defaults to 1.
        enabled (bool, optional): Whether the job is enabled. Defaults to True.
        start_time (time, optional): The time of day when the job should start running.
            If None, the job can run at any time.
        end_time (time, optional): The time of day when the job should stop running.
            If None, the job can run until the next trigger.
        time_zone (timezone, optional): The timezone for the time window. If None, uses
            the system timezone.
    """

    id: str
    name: str
    func: Func[..., Any]
    enabled: bool = True
    trigger: BaseTrigger
    max_instances: int = 1
    start_time: time | None = None
    end_time: time | None = None
    time_zone: timezone | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("id", "name", mode="before")
    @classmethod
    def not_empty_string(cls, v: str) -> str:
        if not v or v.isspace():
            raise ValueError("Name cannot be empty")
        return v

    @field_validator("max_instances", mode="before")
    @classmethod
    def positive_integer(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Max instances must be a positive integer")
        return v

    @field_validator("end_time", mode="after")
    @classmethod
    def validate_time_window(cls, v: time | None, info: ValidationInfo) -> time | None:
        start_time = info.data.get("start_time")
        if start_time and v and v <= start_time:
            raise ValueError("end_time must be after start_time")
        return v

    def __call__(self, *args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Any:
        """
        🚀 Executes the job function and tracks its execution in the summary service.

        Args:
            *args: Positional arguments to pass to the job function.
            **kwargs: Keyword arguments to pass to the job function.

        Returns:
            Any: The result of the job function execution.
        """
        summary_service = JobSummaryService()

        @wraps(self.func)
        def wrapper(*f_args: tuple[Any, ...], **f_kwargs: dict[str, Any]) -> Any:
            try:
                result = self.func(*f_args, **f_kwargs)
                summary_service.add_job(self.id, self.name, True)
                logger.success(f"✅ Job {self.name} executed successfully")
                return result
            except Exception as e:
                summary_service.add_job(self.id, self.name, False, e)
                logger.error(f"❌ Job {self.name} failed: {e}")
                raise e

        return wrapper(*args, **kwargs)
