from abc import ABC, abstractmethod
from concurrent.futures import Future
from enum import Enum
from typing import Any, Optional

from scriptman.powers.generics import Func, P, R


class ExecutorType(str, Enum):
    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"


class ExecutionManager(ABC):
    """
    🔧 Abstract base class for all execution backends

    Defines the contract that all execution managers must follow.
    This allows TaskManager to work with any execution backend.
    """

    @abstractmethod
    def submit_task(
        self, func: Func[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> Future[R]:
        """
        📤 Submit a task for execution

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Future: The future object representing the task

        Raises:
            RuntimeError: If the executor is shutdown
        """
        pass

    @abstractmethod
    def cleanup(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        🧹 Clean up resources

        Args:
            wait: Whether to wait for running tasks to complete
            timeout: Maximum time to wait for shutdown
        """
        pass

    @property
    @abstractmethod
    def is_shutdown(self) -> bool:
        """
        🔍 Check if manager is shutdown

        Returns:
            bool: True if shutdown, False otherwise
        """
        pass

    @property
    @abstractmethod
    def is_idle(self) -> bool:
        """
        🔍 Check if executor has been idle

        Returns:
            bool: True if idle, False otherwise
        """
        pass

    @property
    @abstractmethod
    def last_activity_time(self) -> float:
        """
        🔍 Get last activity timestamp

        Returns:
            float: Timestamp of last activity
        """
        pass

    @property
    @abstractmethod
    def info(self) -> dict[str, Any]:
        """
        🔍 Get comprehensive information about the executor

        Returns:
            dict[str, Any]: Comprehensive information about the executor
        """
        pass

    @abstractmethod
    def update_resource_limits(self, max_workers: int) -> None:
        """
        🔄 Update resource limits

        Args:
            max_workers: New maximum number of workers
        """
        pass
