from concurrent.futures import ALL_COMPLETED, FIRST_EXCEPTION, Future, wait
from dataclasses import dataclass, field
from time import perf_counter
from typing import Generic, Literal, Optional, Union, overload

from scriptman.powers.generics import T


@dataclass
class TaskFuture(Generic[T]):
    """📦 Container for a background task that can be awaited later"""

    _future: Future[T]
    _start_time: float = field(default_factory=perf_counter)

    @overload
    def await_result(self, *, raise_exceptions: Literal[True] = True) -> T:
        """⏱ Await and return the task result, raising an exception if it fails"""
        ...

    @overload
    def await_result(self, *, raise_exceptions: Literal[False]) -> Optional[T]:
        """⏱ Await and return the task result, returning None if it fails"""
        ...

    def await_result(self, *, raise_exceptions: bool = True) -> Union[T, Optional[T]]:
        """
        ⌚ Await and return the task result

        Args:
            raise_exceptions: Whether to raise exceptions that occurred during execution

        Returns:
            The task result if successful, or None if failed and raise_exceptions is False

        Raises:
            Exception: If the task failed and raise_exceptions is True
        """
        try:
            return self._future.result()
        except Exception as e:
            if raise_exceptions:
                raise e
            return None

    @property
    def is_done(self) -> bool:
        """✅ Whether the task has completed (successfully or with error)"""
        return self._future.done()

    @property
    def is_successful(self) -> bool:
        """✅ Whether the task completed successfully"""
        return self.is_done and self.await_result(raise_exceptions=False) is not None

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
class BatchTaskFuture(Generic[T]):
    """📋 Container for multiple parallel tasks that can be awaited together"""

    _tasks: list[TaskFuture[T]] = field(default_factory=list)
    _start_time: float = field(default_factory=perf_counter)

    @overload
    def await_results(
        self, *, raise_exceptions: Literal[True] = True, timeout: Optional[float] = None
    ) -> list[T]:
        """⏱ Await and return results from all tasks, raising an exception if any fail"""
        ...

    @overload
    def await_results(
        self, *, raise_exceptions: Literal[False], timeout: Optional[float] = None
    ) -> list[Optional[T]]:
        """⏱ Await and return results from all tasks, returning None for failed tasks"""
        ...

    def await_results(
        self, *, raise_exceptions: bool = True, timeout: Optional[float] = None
    ) -> Union[list[T], list[Optional[T]]]:
        """
        ⌚ Await and return results from all tasks

        Args:
            raise_exceptions: Whether to raise exceptions that occurred during execution
            timeout: Maximum time to wait (in seconds) or None to wait indefinitely

        Returns:
            List of task results in the same order as the tasks.
            If raise_exceptions is False, failed tasks will be None.

        Raises:
            Exception: If any task failed and raise_exceptions is True
        """
        done, not_done = wait(
            timeout=timeout,
            fs=[task._future for task in self._tasks],
            return_when=ALL_COMPLETED if not raise_exceptions else FIRST_EXCEPTION,
        )

        # If we got here with raise_exceptions=True, all tasks succeeded or none started
        results: list[T | None] = []
        exceptions: list[Exception] = []
        for task in self._tasks:
            try:
                results.append(task.await_result(raise_exceptions=False))
            except Exception as e:
                exceptions.append(e)
                results.append(None)

        if raise_exceptions and exceptions:
            raise exceptions[0]  # Raise the first exception encountered

        return results

    @property
    def are_successful(self) -> bool:
        """✅ Whether all tasks have completed successfully"""
        return all(task.is_successful for task in self._tasks)

    @property
    def is_all_done(self) -> bool:
        """✅ Whether all tasks have completed (successfully or with error)"""
        return all(task.is_done for task in self._tasks)

    @property
    def is_any_done(self) -> bool:
        """✅ Whether any tasks have completed (successfully or with error)"""
        return any(task.is_done for task in self._tasks)

    @property
    def duration(self) -> float:
        """⏰ Batch duration in seconds (from start to now or completion)"""
        if not self.is_all_done:
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
