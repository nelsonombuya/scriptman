from asyncio import get_event_loop
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError,
    wait,
)
from inspect import iscoroutinefunction, signature
from time import perf_counter
from typing import Any, Awaitable, Callable, Literal, Optional

from tqdm import tqdm

from scriptman.powers.concurrency._models import Task, Tasks
from scriptman.powers.generics import P, R


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
    - Named threads for better debugging and monitoring 🔍

    TODO: Future Improvements
    TODO: Task Prioritization: Add support for priority levels to process critical tasks
        first
    TODO: Task Dependencies: Implement DAG-based task dependency management
    TODO: Task Retry Mechanism: Add automatic retry logic with configurable attempts and
        backoff
    TODO: Task Timeout Control: Add per-task timeout settings
    TODO: Task Progress Callbacks: Support custom progress callbacks for flexible
        monitoring
    TODO: Resource Usage Monitoring: Add CPU/memory usage tracking for tasks
    TODO: Task Cancellation: Implement ability to cancel specific running tasks
    TODO: Task Result Caching: Add result caching for expensive operations
    TODO: Task Scheduling: Add support for scheduling tasks for future execution
    TODO: Task Grouping: Implement ability to group related tasks together

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
        self._thread_pool = ThreadPoolExecutor(
            max_workers=thread_pool_size,
            thread_name_prefix="scriptman-task",
        )
        self._process_pool = ProcessPoolExecutor(
            max_workers=process_pool_size,
        )

    def background(self, func: Callable[P, R], *args: Any, **kwargs: Any) -> Task[R]:
        """
        🚀 Run a single task in the background.

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Task: Container that can be awaited to get the result

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
        if iscoroutinefunction(func):
            future = self._thread_pool.submit(self.await_async, func(*args, **kwargs))
        else:
            future = self._thread_pool.submit(func, *args, **kwargs)
        return Task[R](future, args, kwargs, start_time)

    def parallel(
        self,
        tasks: list[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]],
        scope: Literal["multithreading", "multiprocessing"] = "multithreading",
        show_progress: bool = True,
    ) -> Tasks[Any]:
        """
        🔄 Run tasks in parallel using either multithreading or multiprocessing.

        Args:
            tasks: List of (func, args, kwargs) tuples
            scope: Execution scope ("multithreading" or "multiprocessing")
            show_progress: Whether to show a progress bar

        Returns:
            Tasks: Container that manages all tasks together

        Examples:
            # Run tasks in multithreading
            batch = executor.parallel(tasks, scope="multithreading")

            # Run tasks in multiprocessing
            batch = executor.parallel(tasks, scope="multiprocessing")

            # Monitor progress
            print(f"Completed: {batch.completed_count}/{batch.total_count}")

            # Wait for all results with timeout
            try:
                results = batch.await_result(timeout=60.0)
            except TimeoutError:
                print("Some tasks didn't complete in time")
        """
        if scope == "multiprocessing":
            return self.multiprocess(tasks, show_progress)
        return self.multithread(tasks, show_progress)

    def multithread(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool = True,
    ) -> Tasks[R]:
        """
        🌐 Process I/O-bound tasks in parallel using threading.

        Optimized for tasks that spend time waiting for external resources.

        Args:
            tasks: List of (func, args, kwargs) tuples
            show_progress: Whether to show a progress bar

        Returns:
            Tasks: Container that manages all tasks together

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
        batch = Tasks[R]()
        iterator = tqdm(tasks, desc="Threading") if show_progress else tasks

        for func, args, kwargs in iterator:
            start_time = perf_counter()
            future = self._thread_pool.submit(func, *args, **kwargs)
            batch._tasks.append(Task(future, args, kwargs, start_time))

        return batch

    def multiprocess(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool = True,
    ) -> Tasks[R]:
        """
        🔄 Process CPU-intensive tasks in parallel using multiprocessing.

        Optimized for computation-heavy tasks that benefit from multiple CPU cores.

        Args:
            tasks: List of (func, args, kwargs) tuples
            show_progress: Whether to show a progress bar

        Returns:
            Tasks: Container that manages all tasks together

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

        batch = Tasks[R]()
        iterator = tqdm(tasks, desc="Processing") if show_progress else tasks

        for func, args, kwargs in iterator:
            start_time = perf_counter()
            future = self._process_pool.submit(func, *args, **kwargs)
            batch._tasks.append(Task(future, args, kwargs, start_time))

        return batch

    def race(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        *,
        preferred_task_idx: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> Task[R]:
        """
        🏃‍♂️ Race multiple tasks and return the first successful result.

        Args:
            tasks: List of (func, args, kwargs) tuples to race
            preferred_task_idx: If all tasks fail, use this task's result. If None,
                use the result of the task that finishes last.
            timeout: Maximum time to wait for a result

        Returns:
            Task: The winning task's result

        Raises:
            TimeoutError: If no task completes within the specified timeout
            IndexError: If preferred_task_idx is out of range
            ValueError: If tasks list is empty

        Example:
            # Race two tasks and get first successful result
            result = executor.race([
                (check_signatures, (invoice_no,), {}),
                (process_invoice, (invoice_no,), {})
            ], timeout=5.0).await_result()  # Wait up to 5 seconds
        """
        from concurrent.futures import FIRST_COMPLETED, TimeoutError, wait

        assert tasks, "Tasks list cannot be empty"
        assert (timeout is None) or (timeout > 0), "Timeout must be greater than 0"
        assert (preferred_task_idx is None) or (0 <= preferred_task_idx < len(tasks)), (
            f"Preferred task index {preferred_task_idx} "
            f"out of range [0, {len(tasks)})"
        )

        batch = Tasks[R]()
        start_time = perf_counter()
        preferred_task: Optional[Task[R]] = None
        for idx, (func, args, kwargs) in enumerate(tasks):
            task_start = perf_counter()
            future = self._thread_pool.submit(func, *args, **kwargs)
            batch._tasks.append(Task(future, args, kwargs, task_start))

            if preferred_task_idx == idx:
                preferred_task = batch._tasks[idx]
        try:
            while batch._tasks:
                # Calculate remaining timeout
                if timeout is not None:
                    elapsed = perf_counter() - start_time
                    if elapsed >= timeout:
                        raise TimeoutError(f"No task completed within {timeout} seconds")
                    remaining_timeout = timeout - elapsed
                else:
                    remaining_timeout = None

                # Wait for any task to complete
                try:
                    done, pending = wait(
                        [task._future for task in batch._tasks],
                        return_when=FIRST_COMPLETED,
                        timeout=remaining_timeout,
                    )
                except TimeoutError:
                    raise TimeoutError(f"No task completed within {timeout} seconds")

                # Process completed tasks
                successful_tasks = [
                    task
                    for task in batch._tasks
                    if task._future in done and task.is_successful
                ]
                if successful_tasks:
                    return successful_tasks[0]

                # If no successful tasks, check if we have a preferred task
                if preferred_task is not None:
                    if preferred_task._future in done:
                        return preferred_task

                    # Remove all completed tasks except preferred task
                    batch._tasks = [
                        task
                        for task in batch._tasks
                        if task._future in pending or task == preferred_task
                    ]
                    continue

                # If no preferred task, return the result of the task that finishes last
                batch._tasks = [task for task in batch._tasks if task._future in pending]

            # If we somehow get here (shouldn't happen), return the first task
            return batch._tasks[0]
        finally:
            # Cancel any remaining tasks
            for task in batch._tasks:
                if not task._future.done():
                    task._future.cancel()

    def cleanup(self, wait: bool = True) -> None:
        """🧹 Clean up executor resources and shutdown thread/process pools.

        Args:
            wait: Whether to wait for running tasks to complete
        """
        self._thread_pool.shutdown(wait=wait)
        self._process_pool.shutdown(wait=wait)

    @staticmethod
    def await_async[R](awaitable: Awaitable[R]) -> R:
        """
        ⌚ Run an async coroutine synchronously and wait for the result.

        Args:
            awaitable: The coroutine to execute

        Returns:
            The result of the coroutine
        """
        try:
            loop = get_event_loop()
            return loop.run_until_complete(awaitable)
        except RuntimeError as e:
            if "no current event loop" in str(e):
                from asyncio import new_event_loop, set_event_loop

                loop = new_event_loop()
                set_event_loop(loop)
                return loop.run_until_complete(awaitable)
            raise e

    @staticmethod
    def wait(task: Task[R], timeout: Optional[float] = None) -> R:
        """
        ⌚ Run a task synchronously and wait for the result.

        Args:
            task: The task to be waited for.
            timeout: The number of seconds to wait for the task before raising a timeout
                error.

        Returns:
            The result of the task.

        Raises:
            TimeoutError: If the task doesn't complete within the specified about of time.
        """
        done, _ = wait([task._future], timeout=timeout)

        if not done:
            raise TimeoutError(f"Task timed out after {timeout} seconds")

        return task.await_result()

    def __del__(self) -> None:
        """🧹 Clean up executor resources and shutdown thread/process pools."""
        self.cleanup()


__all__: list[str] = ["TaskExecutor", "Task", "Tasks"]
