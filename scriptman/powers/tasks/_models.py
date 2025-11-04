from concurrent.futures import Future
from dataclasses import dataclass, field
from sys import exc_info
from time import perf_counter
from typing import Any, Generic, Iterator, Literal, Optional, overload

from scriptman.powers.generics import T


class TaskException(Exception):
    """
    🚨 Custom exception class for Task errors.

    Args:
        message (str): The concise error message.
        exception (Optional[Exception]): The original exception that occurred.

    Attributes:
        message (str): The concise error message.
        exception (Optional[Exception]): The original exception.
        stacktrace (list[dict]): Structured stacktrace information.
    """

    def __init__(self, message: str, exception: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.message: str = message
        self.exception: Exception = exception or Exception(message)
        self.stacktrace: list[dict[str, str | int | None]] = self.__generate_stacktrace()

    def __generate_stacktrace(self) -> list[dict[str, str | int | None]]:
        """
        📊 Generates a structured stacktrace.

        Returns:
            list[dict]: A list of dictionaries containing stacktrace information.
        """
        from traceback import extract_tb

        return [
            {
                "frame": index,
                "file": frame.filename,
                "line": frame.lineno,
                "function": frame.name,
                "code": frame.line,
            }
            for index, frame in enumerate(extract_tb(exc_info()[2]), 1)
        ]

    def to_dict(self) -> dict[str, Any]:
        """
        📊 Converts the exception to a dictionary representation.

        Returns:
            dict[str, Any]: A dictionary containing exception details.
        """
        return {
            "message": self.message,
            "exception": {
                "type": self.exception.__class__.__name__ if self.exception else None,
                "message": str(self.exception) if self.exception else None,
            },
            "stacktrace": self.stacktrace,
        }

    def __str__(self) -> str:
        """🔍 Get a string representation of the exception"""
        return f"{self.exception.__class__.__name__}: {self.message}"

    def __reduce__(
        self,
    ) -> tuple[type["TaskException"], tuple[Exception], dict[str, Any]]:
        """📦 Enable pickling for this exception class"""
        return (
            TaskException,
            (self.exception,),
            {"message": self.message, "stacktrace": self.stacktrace},
        )

    def __setstate__(self, state: dict[str, Any] | None = None) -> None:
        """📦 Enable unpickling for this exception class"""
        self.message = state.get("message", "") if state is not None else ""
        self.stacktrace = state.get("stacktrace", []) if state is not None else []
        self.exception = Exception(self.message) if self.message else Exception()


@dataclass
class Task(Generic[T]):
    """
    🎯 Task container for managing individual task execution.

    # TODO: Future enhancements
    # - Add cancel() method for task cancellation
    # - Add progress tracking for long-running tasks
    # - Add task dependencies support

    A simplified Task class that wraps a Future and provides essential
    functionality without caching complexity.

    Attributes:
        future: The underlying Future object
        start_time: When the task was started (for duration calculation)
    """

    future: Future[T]
    task_id: Optional[str] = None
    timeout: Optional[float] = None
    start_time: float = field(default_factory=perf_counter)

    @overload
    def await_result(
        self,
        *,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[True] = True,
    ) -> T:
        """⏱ Await and return the task result, raising an exception if it fails"""
        ...

    @overload
    def await_result(
        self,
        *,
        timeout: Optional[float] = None,
        raise_exceptions: bool = False,
    ) -> T | TaskException:
        """⏱ Await and return the task result, returning the exception if it fails"""
        ...

    def await_result(
        self,
        *,
        timeout: Optional[float] = None,
        raise_exceptions: bool = True,
    ) -> T | TaskException:
        """
        ⏱️ Wait for the task to complete and return the result.

        Args:
            timeout: Maximum time to wait for the result
            raise_exceptions: Whether to raise exceptions or return TaskException

        Returns:
            The task result or TaskException if an error occurred

        Raises:
            TimeoutError: If the task doesn't complete within the timeout
            Exception: If raise_exceptions is True and the task failed
        """
        try:
            return self.future.result(timeout)
        except Exception as e:
            if raise_exceptions:
                raise e
            return TaskException(str(e), exception=e)

    def cancel(self) -> bool:
        """🛑 Attempt to cancel the task (succeeds only if not yet started)."""
        try:
            return self.future.cancel()
        except Exception:
            return False

    @property
    def is_done(self) -> bool:
        """✅ Check if the task is completed (successfully or with error)"""
        return self.future.done()

    @property
    def is_successful(self) -> bool:
        """✅ Check if the task completed successfully without errors"""
        return (
            self.future.done()
            and not self.future.cancelled()
            and self.future.exception() is None
        )

    @property
    def duration(self) -> float:
        """⏱️ Get the task execution duration in seconds"""
        return perf_counter() - self.start_time

    @property
    def exception(self) -> Optional[Exception]:
        """🚨 Get the exception that occurred during task execution, if any"""
        exc = self.future.exception()
        return exc if isinstance(exc, Exception) else None

    def __str__(self) -> str:
        """🔍 String representation of the task"""
        status = "completed" if self.is_done else "running"
        return f"Task({status}, duration={self.duration:.2f}s)"


@dataclass
class Tasks(Generic[T]):
    """
    📦 Tasks container for managing multiple task execution.

    A simplified Tasks class that manages a collection of Task objects
    without caching complexity.

    Attributes:
        _tasks: List of Task objects
    """

    _tasks: list[Task[T]] = field(default_factory=list)

    @overload
    def await_results(
        self,
        *,
        lazy: Literal[False] = False,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[True] = True,
        only_successful_results: Literal[True] = True,
    ) -> list[T]:
        """⏱ Await and return results from all tasks, raising an exception if any fail"""
        ...

    @overload
    def await_results(
        self,
        *,
        lazy: Literal[False] = False,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[True] = True,
        only_successful_results: Literal[False] = False,
    ) -> list[T]:
        """⏱ Await and return results from all tasks, raising an exception if any fail"""
        ...

    @overload
    def await_results(
        self,
        *,
        lazy: Literal[False] = False,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[False] = False,
        only_successful_results: Literal[True] = True,
    ) -> list[T]:
        """⏱ Await and return results from all tasks, only returning successful results"""
        ...

    @overload
    def await_results(
        self,
        *,
        lazy: Literal[False] = False,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[False] = False,
        only_successful_results: Literal[False] = False,
    ) -> list[T | TaskException]:
        """
        ⏱ Await and return results from all tasks, returning TaskException for failed
        tasks
        """
        ...

    @overload
    def await_results(
        self,
        *,
        lazy: Literal[True] = True,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[True] = True,
        only_successful_results: Literal[True] = True,
    ) -> Iterator[T]:
        """
        ⏱ Yield results from tasks as they complete, raising an exception if any
        fail
        """
        ...

    @overload
    def await_results(
        self,
        *,
        lazy: Literal[True] = True,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[True] = True,
        only_successful_results: Literal[False] = False,
    ) -> Iterator[T]:
        """
        ⏱ Yield results from tasks as they complete, raising an exception if any fail
        """
        ...

    @overload
    def await_results(
        self,
        *,
        lazy: Literal[True] = True,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[False] = False,
        only_successful_results: Literal[True] = True,
    ) -> Iterator[T]:
        """
        ⏱ Yield results from tasks as they complete, only yielding successful results
        """
        ...

    @overload
    def await_results(
        self,
        *,
        lazy: Literal[True] = True,
        timeout: Optional[float] = None,
        raise_exceptions: Literal[False] = False,
        only_successful_results: Literal[False] = False,
    ) -> Iterator[T | TaskException]:
        """
        ⏱ Yield results from tasks as they complete, yielding TaskException for failed
        tasks
        """
        ...

    def await_results(
        self,
        *,
        lazy: bool = False,
        raise_exceptions: bool = True,
        timeout: Optional[float] = None,
        only_successful_results: bool = False,
    ) -> list[T] | list[T | TaskException] | Iterator[T] | Iterator[T | TaskException]:
        """
        ⏱️ Wait for all tasks to complete and return their results.

        Args:
            timeout: Maximum time to wait for all results
            lazy: Whether to yield results as they complete
            raise_exceptions: Whether to raise exceptions or return TaskException
            only_successful_results: Whether to only return successful results

        Returns:
            If lazy is False:
                List of task results in the same order as the tasks.
                If raise_exceptions is False, failed tasks will be TaskException objects.
                If only_successful_results is True, failed tasks will be excluded.
            If lazy is True:
                Iterator yielding results as tasks complete (not in order).
                If raise_exceptions is False, failed tasks will be TaskException objects.
                If only_successful_results is True, failed tasks will be excluded.

        Raises:
            TimeoutError: If not all tasks complete within the timeout
        """
        if lazy:
            return self.__await_results_lazily(
                timeout=timeout,
                raise_exceptions=raise_exceptions,
                only_successful_results=only_successful_results,
            )
        else:
            return self.__await_results_eagerly(
                timeout=timeout,
                raise_exceptions=raise_exceptions,
                only_successful_results=only_successful_results,
            )

    def __await_results_lazily(
        self,
        *,
        raise_exceptions: bool = True,
        timeout: Optional[float] = None,
        only_successful_results: bool = False,
    ) -> Iterator[T] | Iterator[T | TaskException]:
        """
        ⏱ Yield results as tasks complete.

        Args:
            timeout: Maximum time to wait for results
            raise_exceptions: Whether to raise exceptions or return TaskException
            only_successful_results: Whether to only yield successful results

        Returns:
            Iterator yielding results as tasks complete
        """
        from concurrent.futures import as_completed

        future_to_task = {task.future: task for task in self._tasks}
        futures = [task.future for task in self._tasks]

        for future in as_completed(futures, timeout=timeout):
            task = future_to_task[future]
            result = task.await_result(timeout=timeout, raise_exceptions=raise_exceptions)
            if only_successful_results and isinstance(result, TaskException):
                continue
            yield result

    def __await_results_eagerly(
        self,
        *,
        raise_exceptions: bool = True,
        timeout: Optional[float] = None,
        only_successful_results: bool = False,
    ) -> list[T] | list[T | TaskException]:
        """
        ⏱ Wait for all tasks to complete and return their results.

        Args:
            timeout: Maximum time to wait for results
            raise_exceptions: Whether to raise exceptions or return TaskException
            only_successful_results: Whether to only return successful results

        Returns:
            List of task results in the same order as the tasks.
            If raise_exceptions is False, failed tasks will be TaskException objects.
            If only_successful_results is True, failed tasks will be excluded.
        """
        results: list[T | TaskException] = []
        for task in self._tasks:
            result = task.await_result(timeout=timeout, raise_exceptions=raise_exceptions)
            if only_successful_results and isinstance(result, TaskException):
                continue
            results.append(result)
        return results

    @property
    def completed_count(self) -> int:
        """📊 Number of completed tasks"""
        return sum(1 for task in self._tasks if task.is_done)

    @property
    def total_count(self) -> int:
        """📊 Total number of tasks"""
        return len(self._tasks)

    @property
    def are_successful(self) -> bool:
        """✅ Check if all tasks completed successfully"""
        return all(task.is_successful for task in self._tasks)

    @property
    def running_tasks(self) -> list[Task[T]]:
        """🏃 Get list of running tasks"""
        return [task for task in self._tasks if not task.is_done]

    @property
    def completed_tasks(self) -> list[Task[T]]:
        """📋 Get list of completed tasks"""
        return [task for task in self._tasks if task.is_done]

    @property
    def successful_tasks(self) -> list[Task[T]]:
        """📋 Get list of successful tasks"""
        return [task for task in self._tasks if task.is_successful]

    def __len__(self) -> int:
        """📊 Get the number of tasks"""
        return len(self._tasks)

    def __iter__(self) -> Iterator[Task[T]]:
        """🔄 Iterate over tasks"""
        return iter(self._tasks)

    def __getitem__(self, index: int) -> Task[T]:
        """📋 Get task by index"""
        return self._tasks[index]

    def __str__(self) -> str:
        """🔍 String representation of the tasks"""
        completed = self.completed_count
        total = self.total_count
        return f"Tasks({completed}/{total} completed)"
