try:
    from functools import wraps
    from typing import Any

    from apscheduler.triggers.base import BaseTrigger
    from loguru import logger
    from pydantic import BaseModel, field_validator
    from pydantic.config import ConfigDict

    from scriptman.core.config import config
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
    """

    id: str
    name: str
    func: Func[..., Any]
    enabled: bool = True
    trigger: BaseTrigger
    max_instances: int = 1
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("id", "name", mode="before")
    @classmethod
    def not_empty_string(cls, v: str) -> str:
        if not v or v.isspace():
            raise ValueError("Name cannot be empty")
        return v

    @field_validator("max_instances", mode="before")
    @classmethod
    def not_invalid_max_instances(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Max instances must be greater than 0")
        return v

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        💡 Returns a job info dictionary.

        Returns a dictionary containing the job's details, such as its ID, name, enabled
        status, function name, trigger details, and maximum number of instances.

        The function attribute is wrapped to catch any exceptions and log them with
        the job name.

        Returns:
            dict[str, Any]: A dictionary containing the job's details.
        """

        @wraps(self.func)
        def wrapper(*f_args: Any, **f_kwargs: Any) -> Any:
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logger_id = logger.add(
                f"{config.settings.logs_dir}/{self.name}_{timestamp}.log",
                compression="zip",
                rotation="1 day",
                enqueue=True,
            )

            try:
                logger.info(f"▶️ Executing scheduled job: {self.name}")
                result = self.func(*f_args, **f_kwargs)
                logger.success(f"✅ Job {self.name} executed successfully")
                return result
            except Exception as e:
                logger.error(f"❌ Job {self.name} failed: {e}")
                raise e
            finally:
                logger.remove(logger_id)

        job_details = super().model_dump(*args, **kwargs)
        job_details["func"] = wrapper
        return job_details

    def info_dump(self) -> dict[str, Any]:
        """
        💡 Returns a job info dictionary.

        Returns a dictionary containing the job's details, such as its ID, name, enabled
        status, function name, trigger details, and maximum number of instances.

        Returns:
            dict[str, Any]: A dictionary containing the job's details.
        """
        function_name = (
            self.func.__qualname__
            if hasattr(self.func, "__qualname__")
            else "<unknown_function>"
        )

        trigger_details = {
            "type": self.trigger.__class__.__name__,
            **{
                k: getattr(self.trigger, k)
                for k in self.trigger.__slots__
                if k not in ["__weakref__", "__dict__"]
            },
        }

        return {
            "id": self.id,
            "name": self.name,
            "enabled": self.enabled,
            "func": function_name,
            "trigger": trigger_details,
            "max_instances": self.max_instances,
        }
