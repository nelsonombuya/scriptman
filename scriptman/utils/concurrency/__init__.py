from asyncio import (
    CancelledError,
    Future,
    Task,
    create_task,
    gather,
    iscoroutinefunction,
    run,
    wrap_future,
)
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Coroutine, Optional
from uuid import uuid4

from tqdm import tqdm

from scriptman.utils.concurrency.models import BatchResult, TaskResult, TaskStatus
from scriptman.utils.generics import AsyncFunc, SyncFunc, T


class TaskExecutor:
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
    - Parallel execution with batching
    - Resource cleanup and error handling
    - Task status monitoring and statistics
    """

    def __init__(
        self,
        thread_pool_size: Optional[int] = None,
        process_pool_size: Optional[int] = None,
    ):
        """
        Initialize the TaskExecutor with configurable pool sizes.

        Args:
            thread_pool_size: Maximum number of threads for I/O-bound tasks
            process_pool_size: Maximum number of processes for CPU-bound tasks
        """
        self._background_tasks: dict[str, Task] = {}
        self._thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self._process_pool = ProcessPoolExecutor(max_workers=process_pool_size)

    async def run_in_background(
        self,
        func: SyncFunc[T] | AsyncFunc[T],
        raise_on_error: bool = True,
        *args: tuple,
        **kwargs: dict,
    ) -> str:
        """
        🚀 Run a single task in the background.

        NOTE: The task result can be accessed using the `get_background_result` method.

        Args:
            func: Function to execute
            raise_on_error: Whether to raise an exception if the task fails
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            str : Task ID of the background task
        """
        task_id = f"background_task_{func.__name__}_{uuid4().hex[:8]}"

        if iscoroutinefunction(func):
            coroutine = self._create_task_result(
                task_id=task_id,
                raise_errors=raise_on_error,
                coroutine=func(*args, **kwargs),
            )
        else:
            # Convert synchronous function to coroutine
            coroutine = self._create_task_result(
                task_id=task_id,
                raise_errors=raise_on_error,
                coroutine=wrap_future(self._thread_pool.submit(func, *args, **kwargs)),
            )

        self._background_tasks[task_id] = create_task(coroutine)
        return task_id

    async def get_background_result(self, task_id: str) -> TaskResult:
        """
        📊 Get the result of a background task by its ID.

        Args:
            task_id (str): The task ID of the background task.

        Returns:
            TaskResult: The result of the background task.

        Raises:
            KeyError: If the task ID is not found.
        """
        if task := self._background_tasks.get(task_id):
            try:
                return await task
            except CancelledError as e:
                return TaskResult(task_id=task_id, status=TaskStatus.CANCELLED, error=e)
            finally:
                self._background_tasks.pop(task_id, None)

        raise KeyError(f"💥Task with ID {task_id} not found.")

    def cancel_background_task(self, task_id: str) -> bool:
        """
        🛑 Cancel a background task by its task ID.

        Args:
            task_id (str): The task ID of the background task.

        Returns:
            bool: True if the task was canceled, False otherwise.

        Raises:
            KeyError: If the task ID is not found.
        """
        if task := self._background_tasks.get(task_id):
            return task.cancel()
        raise KeyError(f"💥 Task {task_id} not found.")

    async def parallel_cpu_bound_task(
        self,
        func: SyncFunc[T],
        args: list[tuple] = [],
        kwargs: list[dict] = [],
        batch_size: int = 100,
        batch_id: Optional[str] = None,
        raise_on_error: bool = True,
    ) -> BatchResult[T]:
        """
        🔄 Process CPU-intensive tasks in parallel using multiprocessing.

        Optimized for computation-heavy tasks that benefit from multiple CPU cores.

        Args:
            func: The CPU-bound function to execute
            args: List of argument tuples for each task
            kwargs: List of keyword argument dicts for each task
            batch_size: Number of tasks to process in each batch
            batch_id: Optional identifier for the batch
            raise_on_error: Whether to raise an exception if any task fails

        Returns:
            BatchResult[T]: Results of all processed tasks
        """
        batch_id = batch_id or f"parallel_cpu_{func.__name__}_{uuid4().hex[:8]}"
        start_time = datetime.now()

        # Process tasks in batches to avoid memory issues
        tasks = []
        for i in tqdm(range(0, len(args), batch_size), desc="Processing Tasks"):
            batch_args = args[i : i + batch_size]
            batch_kwargs = kwargs[i : i + batch_size]

            batch_results = await gather(
                *[
                    self._create_task_result(
                        parent_id=batch_id,
                        raise_errors=raise_on_error,
                        coroutine=wrap_future(future),
                        task_id=f"{func.__name__}_{uuid4().hex[:8]}",
                    )
                    for future in [
                        self._process_pool.submit(func, *task_args, **task_kwargs)
                        for task_args, task_kwargs in zip(batch_args, batch_kwargs)
                    ]
                ]
            )
            tasks.extend(batch_results)

        return BatchResult[T](
            tasks=tasks,
            batch_id=batch_id,
            start_time=start_time,
            end_time=datetime.now(),
        )

    async def parallel_io_bound_task(
        self,
        func: SyncFunc[T] | AsyncFunc[T],
        args: list[tuple] = [],
        kwargs: list[dict] = [],
        batch_size: int = 100,
        batch_id: Optional[str] = None,
        raise_on_error: bool = True,
    ) -> BatchResult[T]:
        """🌐 Process I/O-bound tasks in parallel using threading.

        Optimized for tasks that spend time waiting for external resources.

        Args:
            func: The I/O-bound function to execute
            args: List of argument tuples for each task
            kwargs: List of keyword argument dicts for each task
            batch_size: Number of tasks to process in each batch
            batch_id: Optional identifier for the batch
            raise_on_error: Whether to raise an exception if any task fails

        Returns:
            BatchResult[T]: Results of all processed tasks
        """
        batch_id = batch_id or f"parallel_io_{func.__name__}_{uuid4().hex[:8]}"
        start_time = datetime.now()

        # Process tasks in batches to avoid memory issues
        tasks = []
        for i in tqdm(range(0, len(args), batch_size), desc="Processing Tasks"):
            batch_args = args[i : i + batch_size]
            batch_kwargs = kwargs[i : i + batch_size]

            if iscoroutinefunction(func):
                batch_results = await gather(
                    *[
                        self._create_task_result(
                            parent_id=batch_id,
                            raise_errors=raise_on_error,
                            coroutine=func(*task_args, **task_kwargs),
                            task_id=f"{func.__name__}_{uuid4().hex[:8]}",
                        )
                        for task_args, task_kwargs in zip(batch_args, batch_kwargs)
                    ]
                )
            else:
                batch_results = await gather(
                    *[
                        self._create_task_result(
                            parent_id=batch_id,
                            raise_errors=raise_on_error,
                            coroutine=wrap_future(future),
                            task_id=f"{func.__name__}_{uuid4().hex[:8]}",
                        )
                        for future in [
                            self._thread_pool.submit(func, *task_args, **task_kwargs)
                            for task_args, task_kwargs in zip(batch_args, batch_kwargs)
                        ]
                    ]
                )

            tasks.extend(batch_results)

        return BatchResult[T](
            batch_id=batch_id,
            tasks=tasks,
            start_time=start_time,
            end_time=datetime.now(),
        )

    async def _create_task_result(
        self,
        task_id: str,
        coroutine: Future[T] | Coroutine[Any, Any, T],
        parent_id: Optional[str] = None,
        raise_errors: bool = True,
    ) -> TaskResult[T]:
        """Create a task result with proper error handling and timing."""
        result = TaskResult[T](task_id=task_id, parent_id=parent_id)

        try:
            result.status = TaskStatus.RUNNING
            result.start_time = datetime.now()
            result.result = await coroutine
            result.status = TaskStatus.COMPLETED
        except CancelledError as e:
            result.status = TaskStatus.CANCELLED
            result.error = e
            if raise_errors:
                raise
        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = e
            if raise_errors:
                raise
        finally:
            result.end_time = datetime.now()

        return result

    async def cleanup(self, wait: bool = True):
        """🧹 Clean up executor resources and cancel running tasks."""
        if not wait:  # Cancel all running background tasks
            for task in self._background_tasks.values():
                task.cancel()

        if self._background_tasks:  # Wait for all tasks to complete or be cancelled
            await gather(*self._background_tasks.values(), return_exceptions=True)

        # Shutdown thread and process pools
        self._thread_pool.shutdown(wait=True)
        self._process_pool.shutdown(wait=True)

    @staticmethod
    def run_async(coroutine: Coroutine) -> Any:
        """Runs an async coroutine synchronously."""
        return run(coroutine)
