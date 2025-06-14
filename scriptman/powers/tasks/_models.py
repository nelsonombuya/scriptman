from concurrent.futures import ALL_COMPLETED, FIRST_EXCEPTION, Future, wait
from dataclasses import dataclass, field
from time import perf_counter, time
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterator,
    Literal,
    Optional,
    cast,
    overload,
)

from loguru import logger

if TYPE_CHECKING:  # pragma: no cover # NOTE: Avoids circular imports
    from scriptman.powers.cache import CacheManager
    from scriptman.powers.tasks._task_master import TaskMaster

from scriptman.powers.generics import Func, T


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
class TaskSubmission:
    """📋 Internal representation of a submitted task"""

    task_id: str
    func: Func[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    task_type: Literal["cpu", "io", "mixed"] = "mixed"
    priority: int = 0
    submit_time: float = field(default_factory=time)
    promoted: bool = False

    def __lt__(self, other: "TaskSubmission") -> bool:
        """Priority comparison for queue ordering"""
        if self.promoted != other.promoted:
            return self.promoted > other.promoted  # Promoted tasks have higher priority
        if self.priority != other.priority:
            return self.priority > other.priority  # Higher priority values come first
        return self.submit_time < other.submit_time  # FIFO for same priority


@dataclass(frozen=True)
class Task(Generic[T]):
    """📦 Enhanced task container with automatic caching and promotion capabilities"""

    _future: Future[T]
    _task_id: Optional[str] = None  # Task ID for caching and promotion
    _args: tuple[Any, ...] = field(default_factory=tuple)
    _kwargs: dict[str, Any] = field(default_factory=dict)
    _start_time: float = field(default_factory=perf_counter)

    def __hash__(self) -> int:
        """Make Task hashable based on the future object"""
        return hash(self._future)

    def __eq__(self, other: object) -> bool:
        """Compare tasks based on their future objects"""
        if not isinstance(other, Task):
            return NotImplemented
        return self._future is other._future

    @property
    def task_master(self) -> Optional["TaskMaster"]:
        """🎯 Get TaskMaster singleton instance if available"""
        try:
            # Import here to avoid circular imports
            from scriptman.powers.tasks._task_master import TaskMaster

            return TaskMaster.get_instance()
        except (ImportError, AttributeError):
            return None

    @property
    def cache_manager(self) -> Optional["CacheManager"]:
        """💾 Get CacheManager instance if available"""
        try:
            from scriptman.powers.cache import CacheManager

            return CacheManager.get_instance()
        except ImportError:
            return None

    @property
    def task_id(self) -> Optional[str]:
        """🔍 Get the task ID"""
        return self._task_id

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
    def await_result(
        self, *, raise_exceptions: Literal[True] = True, timeout: Optional[float] = None
    ) -> T:
        """⏱ Await and return the task result, raising an exception if it fails"""
        ...

    @overload
    def await_result(
        self, *, raise_exceptions: Literal[False], timeout: Optional[float] = None
    ) -> T | TaskException:
        """⏱ Await and return the task result, returning the exception if it fails"""
        ...

    def await_result(
        self, *, raise_exceptions: bool = True, timeout: Optional[float] = None
    ) -> T | TaskException:
        """
        ⌚ Enhanced await_result with automatic caching and task promotion

        Args:
            raise_exceptions: Whether to raise exceptions that occurred during execution
            timeout: Maximum time to wait for the result

        Returns:
            The task result if successful, or the exception if failed

        Raises:
            Exception: If the task failed and raise_exceptions is True
        """
        # Check cache first if we have a task_id
        if self._task_id:
            cached_result = self._get_cached_result()
            if cached_result is not None:
                # Clean up cache after retrieval
                self._cleanup_cache()

                if isinstance(cached_result, TaskException) and raise_exceptions:
                    raise cached_result.exception
                return cached_result

            # Promote task to foreground for priority processing
            self._promote_task()

        try:
            return self._future.result(timeout=timeout)
        except Exception as e:
            logger.error(
                f"Task {self._task_id or 'unknown'} failed with exception: {e}"
                f"\nArgs: {self._args}"
                f"\nKwargs: {self._kwargs}"
            )
            if raise_exceptions:
                raise TaskException(e)
            return TaskException(e)

    def _get_cached_result(self) -> Optional[T | TaskException]:
        """💾 Get result from cache (memory or disk)"""
        if not self._task_id:
            return None

        # First try memory cache (faster, for non-picklable objects)
        task_master = self.task_master
        if (
            task_master
            and hasattr(task_master, "_memory_cache")
            and self._task_id in task_master._memory_cache
        ):
            logger.debug(f"📦 Memory cache hit for task {self._task_id[:8]}")
            return cast(T | TaskException, task_master._memory_cache[self._task_id])

        # Then try disk cache (for picklable objects)
        if cache_manager := self.cache_manager:
            cached = cache_manager.get(self._task_id)
            if cached is not None:
                logger.debug(f"💾 Disk cache hit for task {self._task_id[:8]}")
                return cast(T | TaskException, cached)

        return None

    def _cache_result(self, result: T | TaskException) -> None:
        """💾 Cache result in appropriate storage"""
        if not self._task_id:
            return

        # Try disk cache first (preferred for persistence)
        if cache_manager := self.cache_manager:
            try:
                if cache_manager.set(self._task_id, result):
                    logger.debug(f"💾 Cached task {self._task_id[:8]} result to disk")
                    return
            except Exception as e:
                logger.debug(f"💾 Failed to cache to disk: {e}, falling back to memory")

        # Fallback to memory cache for non-picklable objects
        if task_master := self.task_master:
            task_master._memory_cache[self._task_id] = result
            logger.debug(f"📦 Cached task {self._task_id} result to memory")

    def _cleanup_cache(self) -> None:
        """🧹 Clean up cache after result retrieval"""
        if not self._task_id:
            return

        # Clean memory cache
        if task_master := self.task_master:
            if self._task_id in task_master._memory_cache:
                del task_master._memory_cache[self._task_id]
                logger.debug(f"🧹 Cleaned memory cache for task {self._task_id}")

        # Clean disk cache
        if cache_manager := self.cache_manager:
            try:
                cache_manager.delete(self._task_id)
                logger.debug(f"💾 Cleaned disk cache for task {self._task_id}")
            except Exception as e:
                logger.debug(f"Failed to clean disk cache: {e}")

    def _promote_task(self) -> None:
        """⚡ Promote task to foreground for priority processing"""
        if not self._task_id:
            return

        if task_master := self.task_master:
            task_master.promote_task(self._task_id)

    @property
    def is_done(self) -> bool:
        """✅ Whether the task has completed (successfully or with error)"""
        # Check cache first
        if self._task_id and self._get_cached_result() is not None:
            return True
        return self._future.done()

    @property
    def is_successful(self) -> bool:
        """✅ Whether the task completed successfully"""
        if not self.is_done:
            return False

        result = self.await_result(raise_exceptions=False)
        return not isinstance(result, TaskException)

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
        lazy: Literal[False] = False,
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
        only_successful_results: bool = False,
    ) -> list[T] | list[T | TaskException] | Iterator[T] | Iterator[T | TaskException]:
        """
        ⌚ Await and return results from all tasks

        Args:
            lazy: Whether to yield results as they complete instead of returning a list
            raise_exceptions: Whether to raise exceptions that occurred during execution
            only_successful_results: Whether to filter out TaskException results

        Returns:
            If lazy is False:
                List of task results in the same order as the tasks.
                If raise_exceptions is False, failed tasks will be TaskException objects.
            If lazy is True:
                Iterator yielding results as tasks complete (not in order).
                If raise_exceptions is False, failed tasks will yield TaskException
                    objects.

        Raises:
            Exception: If any task failed and raise_exceptions is True
        """
        if lazy:
            return self._await_results_lazy(
                raise_exceptions=raise_exceptions,
                only_successful_results=only_successful_results,
            )

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

    def _await_results_lazy(
        self,
        *,
        raise_exceptions: bool,
        only_successful_results: bool,
    ) -> Iterator[T] | Iterator[T | TaskException]:
        """
        Internal method to yield results as tasks complete
        """
        exceptions: list[Exception] = []
        futures: list[Future[T]] | set[Future[T]]
        futures = [task._future for task in self._tasks]

        while futures:
            done, futures = wait(
                fs=futures,
                return_when=FIRST_EXCEPTION if raise_exceptions else ALL_COMPLETED,
            )

            for future in done:
                task = next(t for t in self._tasks if t._future is future)
                result = task.await_result(raise_exceptions=False)

                if isinstance(result, TaskException):
                    exceptions.append(result.exception)
                    if not only_successful_results:
                        yield result
                else:
                    yield result

            if raise_exceptions and exceptions:
                raise exceptions[0]  # Raise the first exception encountered

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
