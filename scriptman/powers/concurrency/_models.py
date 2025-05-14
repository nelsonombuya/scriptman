from concurrent.futures import ALL_COMPLETED, FIRST_EXCEPTION, Future, wait
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Generic, Iterator, Literal, overload

from loguru import logger

from scriptman.powers.generics import T


class TaskException(Exception):
    """🚨 Serializable exception wrapper for task failures"""

    def __init__(self, exception: Exception) -> None:
        self.exception = exception
        self.message = str(exception)
        super().__init__(self.message)
        self.exception_type = exception.__class__.__name__
        self.stacktrace = getattr(exception, "__traceback__", None)

    def __str__(self) -> str:
        return self.message

    def __reduce__(
        self,
    ) -> tuple[type["TaskException"], tuple[Exception], dict[str, Any]]:
        """Enable pickling for this exception class"""
        return (
            self.__class__,
            (Exception(self.message),),
            {"exception_type": self.exception_type, "stacktrace": None},
        )


@dataclass
class Task(Generic[T]):
    """📦 Container for a background task that can be awaited later"""

    _future: Future[T]
    _args: tuple[Any, ...] = field(default_factory=tuple)
    _kwargs: dict[str, Any] = field(default_factory=dict)
    _start_time: float = field(default_factory=perf_counter)

    @property
    def result(self) -> T:
        """🔍 Get the task result"""
        return self.await_result()

    @property
    def exception(self) -> Exception | None:
        """🔍 Get the task exception"""
        result = self.await_result(raise_exceptions=False)
        return result.exception if isinstance(result, TaskException) else None

    @overload
    def await_result(self, *, raise_exceptions: Literal[True] = True) -> T:
        """⏱ Await and return the task result, raising an exception if it fails"""
        ...

    @overload
    def await_result(self, *, raise_exceptions: Literal[False]) -> T | TaskException:
        """⏱ Await and return the task result, returning the exception if it fails"""
        ...

    def await_result(self, *, raise_exceptions: bool = True) -> T | TaskException:
        """
        ⌚ Await and return the task result

        Args:
            raise_exceptions: Whether to raise exceptions that occurred during execution

        Returns:
            The task result if successful, or the exception if failed

        Raises:
            Exception: If the task failed and raise_exceptions is True
        """
        try:
            return self._future.result()
        except Exception as e:
            logger.error(
                f"Task failed with exception: {e}"
                f"\nArgs: {self._args}"
                f"\nKwargs: {self._kwargs}"
            )
            if raise_exceptions:
                raise e
            return TaskException(e)

    @property
    def is_done(self) -> bool:
        """✅ Whether the task has completed (successfully or with error)"""
        return self._future.done()

    @property
    def is_successful(self) -> bool:
        """✅ Whether the task completed successfully"""
        result = self.await_result(raise_exceptions=False)
        return self.is_done and not isinstance(result, TaskException)

    @property
    def duration(self) -> float:
        """⏰ Task duration in seconds (if completed)"""
        from time import perf_counter

        if not self.is_done:
            # For running tasks, simply return current duration
            return perf_counter() - self._start_time

        # For completed tasks, we want to ensure we capture the actual completion time
        if not hasattr(self._future, "_completion_timestamp"):
            try:
                # Get the result without blocking since we know it's done
                self._future.result(timeout=0)
            except Exception:
                pass  # Task failed, but we still want to track when it completed
            finally:
                # Store completion time regardless of success/failure
                setattr(self._future, "_completion_timestamp", perf_counter())

        return float(getattr(self._future, "_completion_timestamp")) - self._start_time


@dataclass
class Tasks(Generic[T]):
    """📋 Container for multiple parallel tasks that can be awaited together"""

    _tasks: list[Task[T]] = field(default_factory=list)
    _start_time: float = field(default_factory=perf_counter)

    def __getitem__(self, index: int) -> Task[T]:
        return self._tasks[index]

    def __iter__(self) -> Iterator[Task[T]]:
        return iter(self._tasks)

    def __len__(self) -> int:
        return len(self._tasks)

    @property
    def results(self) -> list[T]:
        return [task.result for task in self._tasks]

    @property
    def exceptions(self) -> list[Exception | None]:
        return [task.exception for task in self._tasks]

    @overload
    def await_results(
        self,
        *,
        raise_exceptions: Literal[True] = True,
        only_successful_results: Literal[True] = True,
    ) -> list[T]:
        """⏱ Await and return results from all tasks, raising an exception if any fail"""
        ...

    @overload
    def await_results(
        self,
        *,
        raise_exceptions: Literal[True] = True,
        only_successful_results: Literal[False] = False,
    ) -> list[T]:
        """⏱ Await and return results from all tasks, raising an exception if any fail"""
        ...

    @overload
    def await_results(
        self,
        *,
        raise_exceptions: Literal[False] = False,
        only_successful_results: Literal[True] = True,
    ) -> list[T]:
        """⏱ Await and return results from all tasks, only returning successful results"""
        ...

    @overload
    def await_results(
        self,
        *,
        raise_exceptions: Literal[False] = False,
        only_successful_results: Literal[False] = False,
    ) -> list[T | TaskException]:
        """
        ⏱ Await and return results from all tasks, returning TaskException for failed
        tasks
        """
        ...

    def await_results(
        self,
        *,
        raise_exceptions: bool = True,
        only_successful_results: bool = False,
    ) -> list[T] | list[T | TaskException]:
        """
        ⌚ Await and return results from all tasks

        Args:
            raise_exceptions: Whether to raise exceptions that occurred during execution
            only_successful_results: Whether to filter out TaskException results

        Returns:
            List of task results in the same order as the tasks.
            If raise_exceptions is False, failed tasks will be TaskException objects.

        Raises:
            Exception: If any task failed and raise_exceptions is True
        """
        done, not_done = wait(
            fs=[task._future for task in self._tasks],
            return_when=ALL_COMPLETED if not raise_exceptions else FIRST_EXCEPTION,
        )

        # If we got here with raise_exceptions=True, all tasks succeeded or none started
        results: list[T | TaskException] = []
        exceptions: list[Exception] = []

        for task in self._tasks:
            result = task.await_result(raise_exceptions=False)
            if isinstance(result, TaskException):
                exceptions.append(result.exception)
                if not only_successful_results:
                    results.append(result)
            else:
                results.append(result)

        if raise_exceptions and exceptions:
            raise exceptions[0]  # Raise the first exception encountered

        return results

    @property
    def are_successful(self) -> bool:
        """✅ Whether all tasks have completed successfully"""
        return all(task.is_successful for task in self._tasks)

    @property
    def are_done(self) -> bool:
        """✅ Whether all tasks have completed (successfully or with error)"""
        return all(task.is_done for task in self._tasks)

    @property
    def is_any_done(self) -> bool:
        """✅ Whether any tasks have completed (successfully or with error)"""
        return any(task.is_done for task in self._tasks)

    @property
    def duration(self) -> float:
        """⏰ Batch duration in seconds (from start to now or completion)"""
        if not self.are_done:
            return perf_counter() - self._start_time

        # Get the latest completion time among all tasks
        latest_finish = max(task.duration + self._start_time for task in self._tasks)
        return latest_finish - self._start_time

    @property
    def completed_count(self) -> int:
        """📊 Number of completed tasks"""
        return sum(1 for task in self._tasks if task.is_done)

    @property
    def total_count(self) -> int:
        """📊 Total number of tasks"""
        return len(self._tasks)

    @property
    def successful_count(self) -> int:
        """📊 Number of successful tasks"""
        return sum(1 for task in self._tasks if task.is_successful)

    @property
    def failure_count(self) -> int:
        """📊 Number of failed tasks"""
        return sum(1 for task in self._tasks if not task.is_successful)
