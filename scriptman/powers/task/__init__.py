from asyncio import get_event_loop, iscoroutinefunction
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from inspect import signature
from time import perf_counter
from typing import Any, Awaitable, Callable, Optional

from tqdm import tqdm

from scriptman.powers.generics import P, R
from scriptman.powers.task._models import BatchTaskFuture, TaskFuture


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
    - Background execution with awaitable task futures ⏱️
    - Efficient parallel execution with thread and process pools ⚡
    - Elegant handling of errors with customizable exception behavior 🛡️
    - Comprehensive task monitoring with duration and status tracking 📊
    - Resource cleanup and management 🧹

    Examples:
        # Run a single task in background
        task = executor.background(slow_function, arg1, arg2)
        # Do other work...
        result = task.await_result()

        # Run multiple I/O-bound tasks in parallel
        batch = executor.multithread([
            (fetch_url, ("https://api1.com",), {}),
            (fetch_url, ("https://api2.com",), {"timeout": 30}),
        ])
        results = batch.await_result()  # List of results in same order

        # Run CPU-intensive tasks with progress bar
        batch = executor.multiprocess([
            (process_image, (image1,), {"quality": "high"}),
            (process_image, (image2,), {"quality": "medium"}),
        ])
        print(f"Completed: {batch.completed_count}/{batch.total_count}")
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
        self._thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self._process_pool = ProcessPoolExecutor(max_workers=process_pool_size)

    def background(
        self, func: Callable[P, R], *args: Any, **kwargs: Any
    ) -> TaskFuture[R]:
        """
        🚀 Run a single task in the background.

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            TaskFuture: Container that can be awaited to get the result

        Examples:
            # Start a task in the background
            task = executor.background(slow_function, "arg1", kwarg=123)

            # Check if it's done
            if task.is_done:
                print(f"Task completed in {task.duration:.2f} seconds")

            # Get the result when needed
            result = task.await_result()
        """
        start_time = perf_counter()
        future = self._thread_pool.submit(func, *args, **kwargs)
        return TaskFuture[R](future, start_time)

    def multithread(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool = True,
    ) -> BatchTaskFuture[R]:
        """
        🌐 Process I/O-bound tasks in parallel using threading.

        Optimized for tasks that spend time waiting for external resources.

        Args:
            tasks: List of (func, args, kwargs) tuples
            show_progress: Whether to show a progress bar

        Returns:
            BatchTaskFuture: Container that manages all tasks together

        Examples:
            # Run multiple API calls in parallel
            batch = executor.multithread([
                (fetch_url, ("https://api1.com",), {}),
                (fetch_url, ("https://api2.com",), {"timeout": 30}),
            ])

            # Wait for all results
            results = batch.await_result()

            # Or ignore errors and get partial results
            results = batch.await_result(raise_exceptions=False)
        """
        batch = BatchTaskFuture[R]()
        iterator = tqdm(tasks, desc="Threading") if show_progress else tasks

        for func, args, kwargs in iterator:
            start_time = perf_counter()
            future = self._thread_pool.submit(func, *args, **kwargs)
            batch._tasks.append(TaskFuture(future, start_time))

        return batch

    def multiprocess(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool = True,
    ) -> BatchTaskFuture[R]:
        """
        🔄 Process CPU-intensive tasks in parallel using multiprocessing.

        Optimized for computation-heavy tasks that benefit from multiple CPU cores.

        Args:
            tasks: List of (func, args, kwargs) tuples
            show_progress: Whether to show a progress bar

        Returns:
            BatchTaskFuture: Container that manages all tasks together

        Examples:
            # Process multiple images in parallel
            batch = executor.multiprocess([
                (process_image, (image1,), {"quality": "high"}),
                (process_image, (image2,), {"quality": "medium"}),
            ])

            # Monitor progress
            print(f"Completed: {batch.completed_count}/{batch.total_count}")

            # Wait for all results with timeout
            try:
                results = batch.await_result(timeout=60.0)
            except TimeoutError:
                print("Some tasks didn't complete in time")
        """
        # Check if any function is a method (which can't be pickled)
        for func, _, _ in tasks:
            param_names = list(signature(func).parameters.keys())
            if param_names and param_names[0] in ("self", "cls"):
                raise ValueError(
                    f"Cannot use multiprocess with instance or class method "
                    f"{func.__name__}. Methods with 'self' or 'cls' parameters cannot be "
                    "pickled for multiprocessing. Consider using a standalone function "
                    "or static method."
                )
            if iscoroutinefunction(func):
                raise ValueError(
                    f"Cannot use multiprocess with coroutine function {func.__name__}. "
                    "Async functions are not supported with multiprocessing."
                )

        batch = BatchTaskFuture[R]()
        iterator = tqdm(tasks, desc="Processing") if show_progress else tasks

        for func, args, kwargs in iterator:
            start_time = perf_counter()
            future = self._process_pool.submit(func, *args, **kwargs)
            batch._tasks.append(TaskFuture(future, start_time))

        return batch

    def cleanup(self, wait: bool = True) -> None:
        """🧹 Clean up executor resources and shutdown thread/process pools.

        Args:
            wait: Whether to wait for running tasks to complete
        """
        self._thread_pool.shutdown(wait=wait)
        self._process_pool.shutdown(wait=wait)

    @staticmethod
    def await_async[R](awaitable: Awaitable[R]) -> R:
        """⌚ Run an async coroutine synchronously and wait for the result.

        Args:
            awaitable: The coroutine to execute

        Returns:
            The result of the coroutine
        """
        loop = get_event_loop()
        try:
            return loop.run_until_complete(awaitable)
        except RuntimeError as e:
            if "no current event loop" in str(e):
                from asyncio import new_event_loop, set_event_loop

                loop = new_event_loop()
                set_event_loop(loop)
                return loop.run_until_complete(awaitable)
            raise e

    def __del__(self) -> None:
        """🧹 Clean up executor resources and shutdown thread/process pools."""
        self.cleanup()


__all__: list[str] = ["TaskExecutor", "TaskFuture", "BatchTaskFuture"]
