from asyncio import get_event_loop, iscoroutinefunction
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from functools import partial
from itertools import zip_longest
from typing import Any, Awaitable, Generic, Optional, cast
from uuid import uuid4

from tqdm import tqdm

from scriptman.powers.concurrency._models import BatchResult, TaskResult, TaskStatus
from scriptman.powers.generics import AsyncFunc, Func, SyncFunc, T


class TaskExecutor(Generic[T]):
    """🧩 Task Executor

    Efficiently manages parallel task execution using both threading and multiprocessing
    based on task type:

    🔄 CPU-bound tasks (ProcessPoolExecutor):
    - Data transformation and processing
    - Complex calculations and algorithms
    - Image/video processing
    - Machine learning inference
    - Data compression/decompression

    🌐 I/O-bound tasks (ThreadPoolExecutor):
    - API calls and network requests
    - Database operations
    - File system operations
    - Message queue interactions
    - External service communications

    Features:
    - Automatic task type routing
    - Background execution with task management
    - Efficient parallel execution
    - Resource cleanup and error handling
    - Task status monitoring and statistics
    """

    def __init__(
        self,
        thread_pool_size: Optional[int] = None,
        process_pool_size: Optional[int] = None,
    ):
        """
        🚀 Initialize the TaskExecutor with configurable pool sizes.

        Args:
            thread_pool_size: Maximum number of threads for I/O-bound tasks
            process_pool_size: Maximum number of processes for CPU-bound tasks
        """
        self._background_tasks: dict[str, Future[TaskResult[T]]] = {}
        self._thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self._process_pool = ProcessPoolExecutor(max_workers=process_pool_size)

    def _generate_task_id(self, prefix: str, func_name: str) -> str:
        """🆔 Task ID generation"""
        return f"{prefix}_{func_name}_{uuid4().hex[:8]}"

    def run_in_background(self, func: Func[T], *args: Any, **kwargs: Any) -> str:
        """
        🚀 Run a single task in the background.

        NOTE: The task result can be accessed using the `get_background_result` method.

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            str : Task ID of the background task
        """
        task_id = self._generate_task_id("background_task", func.__name__)
        bound_func = partial(self._execute_task, func, args, kwargs, task_id)
        self._background_tasks[task_id] = self._thread_pool.submit(bound_func)
        return task_id

    def get_background_result(self, task_id: str) -> TaskResult[T]:
        """
        📊 Get the result of a background task by its ID.

        Args:
            task_id: The task ID of the background task.

        Returns:
            TaskResult: The result of the background task.

        Raises:
            KeyError: If the task ID is not found.
        """
        if future := self._background_tasks.get(task_id):
            try:
                return future.result()
            finally:
                self._background_tasks.pop(task_id, None)

        raise KeyError(f"💥 Task {task_id} not found.")

    def parallel_cpu_bound_task(
        self,
        func: SyncFunc[T],
        args: list[tuple[Any, ...]] = [],
        kwargs: list[dict[str, Any]] = [],
        raise_on_error: bool = True,
    ) -> BatchResult[T]:
        """
        🔄 Process CPU-intensive tasks in parallel using multiprocessing.

        Optimized for computation-heavy tasks that benefit from multiple CPU cores.

        Args:
            func: The CPU-bound function to execute
            args: List of argument tuples for each task
            kwargs: List of keyword argument dicts for each task
            raise_on_error: Whether to raise an exception if any task fails

        Returns:
            BatchResult[T]: Results of all processed tasks
        """
        assert not iscoroutinefunction(func), "CPU-bound tasks should be synchronous."
        batch_id = self._generate_task_id("parallel_cpu", func.__name__)
        start_time = datetime.now()

        futures = []
        for args_tuple, kwargs_dict in tqdm(
            self._zip_args_and_kwargs(args, kwargs),
            total=max(len(args), len(kwargs)),
            desc="Processing CPU bound tasks",
            unit="task",
        ):
            task_id = self._generate_task_id(func.__name__, uuid4().hex[:8])
            bound_func = partial(
                self._execute_task, func, args_tuple, kwargs_dict, task_id, batch_id
            )
            futures.append(self._process_pool.submit(bound_func))

        results = []
        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                if raise_on_error:
                    raise
                # Create failed task result
                results.append(
                    TaskResult(
                        error=e,
                        parent_id=batch_id,
                        status=TaskStatus.FAILED,
                        task_id=self._generate_task_id("failed", func.__name__),
                    )
                )

        return BatchResult[T](
            tasks=results,
            batch_id=batch_id,
            start_time=start_time,
            end_time=datetime.now(),
        )

    def parallel_io_bound_task(
        self,
        func: Func[T],
        args: list[tuple[Any, ...]] = [],
        kwargs: list[dict[str, Any]] = [],
        raise_on_error: bool = True,
    ) -> BatchResult[T]:
        """
        🌐 Process I/O-bound tasks in parallel using threading.

        Optimized for tasks that spend time waiting for external resources.

        Args:
            func: The I/O-bound function to execute
            args: List of argument tuples for each task
            kwargs: List of keyword argument dicts for each task
            raise_on_error: Whether to raise an exception if any task fails

        Returns:
            BatchResult[T]: Results of all processed tasks
        """
        batch_id = self._generate_task_id("parallel_io", func.__name__)
        start_time = datetime.now()

        futures = []
        for args_tuple, kwargs_dict in tqdm(
            self._zip_args_and_kwargs(args, kwargs),
            total=max(len(args), len(kwargs)),
            desc="Processing IO bound tasks",
            unit="task",
        ):
            task_id = self._generate_task_id(func.__name__, uuid4().hex[:8])
            bound_func = partial(
                self._execute_task, func, args_tuple, kwargs_dict, task_id, batch_id
            )
            futures.append(self._thread_pool.submit(bound_func))

        results = []
        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                if raise_on_error:
                    raise
                # Create failed task result
                results.append(
                    TaskResult(
                        error=e,
                        parent_id=batch_id,
                        status=TaskStatus.FAILED,
                        task_id=self._generate_task_id("failed", func.__name__),
                    )
                )

        return BatchResult[T](
            tasks=results,
            batch_id=batch_id,
            start_time=start_time,
            end_time=datetime.now(),
        )

    def _execute_task(
        self,
        func: Func[T],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task_id: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> TaskResult[T]:
        """Execute a single task and return its result with metadata"""
        result = TaskResult[T](
            args=args,
            kwargs=kwargs,
            parent_id=parent_id,
            task_id=task_id or self._generate_task_id("Executor", func.__name__),
        )

        try:
            result.status = TaskStatus.RUNNING
            result.start_time = datetime.now()

            if iscoroutinefunction(func):
                future = cast(AsyncFunc[T], func)(*args, **kwargs)
                result.result = self.await_async(future)
            else:
                result.result = cast(SyncFunc[T], func)(*args, **kwargs)

            result.status = TaskStatus.COMPLETED
        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = e
            raise
        finally:
            result.end_time = datetime.now()

        return result

    def cleanup(self, wait: bool = True) -> None:
        """🧹 Clean up executor resources and cancel running tasks."""
        if not wait:  # Cancel all running background tasks
            for future in self._background_tasks.values():
                future.cancel()

        # Shutdown thread and process pools
        self._thread_pool.shutdown(wait=wait)
        self._process_pool.shutdown(wait=wait)

    @staticmethod
    def await_async(awaitable: Awaitable[T]) -> T:
        """⌚ Runs an async coroutine synchronously and waits for the result."""
        loop = get_event_loop()
        try:
            return loop.run_until_complete(awaitable)
        except RuntimeError as e:
            if "no current event loop" in str(e):
                loop = get_event_loop()
                return loop.run_until_complete(awaitable)
            raise

    @staticmethod
    def _zip_args_and_kwargs(
        args_list: list[tuple[Any, ...]],
        kwargs_list: list[dict[str, Any]],
    ) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
        """🤐 Combine lists of positional and keyword arguments efficiently"""
        empty_dict: dict[str, Any] = {}
        empty_tuple: tuple[Any, ...] = ()
        return [
            (
                args if isinstance(args, tuple) else empty_tuple,
                kwargs if isinstance(kwargs, dict) else empty_dict,
            )
            for args, kwargs in zip_longest(
                args_list,
                kwargs_list,
                fillvalue=empty_tuple if not args_list else empty_dict,
            )
        ]


__all__: list[str] = ["TaskExecutor", "TaskStatus", "TaskResult", "BatchResult"]
