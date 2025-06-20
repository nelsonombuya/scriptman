from concurrent.futures import (
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError,
    wait,
)
from inspect import iscoroutinefunction, signature
from threading import Event
from time import perf_counter, sleep, time
from typing import Any, Awaitable, Callable, Literal, Optional

from loguru import logger
from tqdm import tqdm

from scriptman.powers.generics import P, R
from scriptman.powers.tasks._models import Task, Tasks
from scriptman.powers.tasks._task_master import TaskMaster


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
    - Flexible execution modes: Smart mode (intelligent) or Direct mode (direct) 🎯

    Execution Modes:
    - Smart mode: Uses intelligent task management with caching, resource scaling,
        and hybrid execution (default)
    - Direct mode: Uses direct thread/process pools for traditional execution

    TODO: Future Improvements
    TODO: Task Dependencies: Implement DAG-based task dependency management
    TODO: Task Retry Mechanism: Add automatic retry logic with configurable attempts and
        backoff
    TODO: Task Progress Callbacks: Support custom progress callbacks for flexible
        monitoring
    TODO: Task Cancellation: Implement ability to cancel specific running tasks
    TODO: Task Grouping: Implement ability to group related tasks together

    Examples:
        # Smart mode (intelligent management)
        executor = TaskExecutor(mode="smart")
        task = executor.background(slow_function, arg1, arg2)
        result = task.await_result()

        # Direct mode (traditional execution)
        executor = TaskExecutor(mode="direct")
        batch = executor.multithread([
            (fetch_url, ("https://api1.com",), {}),
            (fetch_url, ("https://api2.com",), {"timeout": 30}),
        ])
        results = batch.await_result()

        # CPU-intensive tasks with progress bar
        batch = executor.multiprocess([
            (process_image, (image1,), {"quality": "high"}),
            (process_image, (image2,), {"quality": "medium"}),
        ])
        print(f"Completed: {batch.completed_count}/{batch.total_count}")
    """

    # Class-level shutdown event shared across all instances
    _shutdown_event: Event = Event()
    _active_tasks: set[Task[Any]] = set()

    def __init__(
        self,
        mode: Literal["smart", "direct"] = "smart",
        thread_pool_size: Optional[int] = None,
        process_pool_size: Optional[int] = None,
    ):
        """
        🚀 Initialize the TaskExecutor with configurable execution mode and pool sizes.

        Args:
            mode: Execution mode ("smart" for intelligent management,
                "direct" for direct execution)
            thread_pool_size: Maximum number of threads for I/O-bound tasks
                (direct mode only)
            process_pool_size: Maximum number of processes for CPU-bound tasks
                (direct mode only)
        """
        self._mode = mode
        self._log = logger

        if mode == "smart":
            # Use TaskMaster for intelligent task management
            self._task_master: Optional[TaskMaster] = TaskMaster.get_instance()
            self._process_pool: Optional[ProcessPoolExecutor] = None
            self._thread_pool: Optional[ThreadPoolExecutor] = None
            self._log.info("🎯 TaskExecutor initialized in Smart mode")
        else:
            # Use direct thread/process pools for direct execution
            self._task_master = None
            self._thread_pool = ThreadPoolExecutor(thread_pool_size, "task_executor")
            self._process_pool = ProcessPoolExecutor(process_pool_size)
            self._log.info("🔧 TaskExecutor initialized in Direct mode")

    def _create_task(
        self,
        future: Future[R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        start_time: float,
    ) -> Task[R]:
        """
        💪🏾 Helper method to create and track a task.

        Args:
            future: The future object for the task
            args: Positional arguments for the task
            kwargs: Keyword arguments for the task
            start_time: When the task was started

        Returns:
            Task: The created task
        """
        task = Task(future, None, args, kwargs, start_time)
        self._active_tasks.add(task)
        future.add_done_callback(lambda _: self._active_tasks.discard(task))
        return task

    def background(self, func: Callable[P, R], *args: Any, **kwargs: Any) -> Task[R]:
        """
        🚀 Run a single task in the background.

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Task: Container that can be awaited to get the result, or an empty task if
                shutting down.

        Examples:
            # Smart mode - intelligent management
            task = executor.background(slow_function, "arg1", task_type="cpu", kwarg=123)

            # Direct mode - direct execution
            task = executor.background(slow_function, "arg1", kwarg=123)

            # Check if it's done
            if task.is_done:
                print(f"Task completed in {task.duration:.2f} seconds")

            # Get the result when needed
            result = task.await_result()
        """
        if self._shutdown_event.is_set():
            self._log.warning("TaskExecutor is shutting down, returning empty Task.")
            return Task(Future())

        if self._mode == "smart" and self._task_master:
            # Use TaskMaster for intelligent task management
            return self._task_master.submit(func, *args, **kwargs)
        else:
            # Use direct execution (original behavior)
            if not self._thread_pool:
                raise RuntimeError("TaskExecutor not initialized for direct mode.")

            start_time = perf_counter()
            if iscoroutinefunction(func):
                future = self._thread_pool.submit(self.await_async, func(*args, **kwargs))
            else:
                future = self._thread_pool.submit(func, *args, **kwargs)

            return self._create_task(future, args, kwargs, start_time)

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
        if self._shutdown_event.is_set():
            self._log.warning("TaskExecutor is shutting down, returning empty Tasks")
            return Tasks()

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
        if self._shutdown_event.is_set():
            self._log.warning("TaskExecutor is shutting down, returning empty Tasks")
            return Tasks()

        if self._mode == "smart" and self._task_master:
            # Use TaskMaster for intelligent task management
            batch = Tasks[R]()
            iterator = tqdm(tasks, desc="Smart Threading") if show_progress else tasks

            for func, args, kwargs in iterator:
                task: Task[R] = self._task_master.submit(func, *args, **kwargs)
                batch._tasks.append(task)

            return batch
        else:
            # Use direct execution (original behavior)
            if not self._thread_pool:
                raise RuntimeError("TaskExecutor not initialized for direct mode")

            batch = Tasks[R]()
            iterator = tqdm(tasks, desc="Direct Threading") if show_progress else tasks

            for func, args, kwargs in iterator:
                start_time = perf_counter()
                future = self._thread_pool.submit(func, *args, **kwargs)
                task = self._create_task(future, args, kwargs, start_time)
                batch._tasks.append(task)

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
        if self._shutdown_event.is_set():
            self._log.warning("TaskExecutor is shutting down, returning empty Tasks")
            return Tasks()

        if self._mode == "smart" and self._task_master:
            # Use TaskMaster for intelligent task management
            batch = Tasks[R]()
            iterator = tqdm(tasks, desc="Smart Processing") if show_progress else tasks

            for func, args, kwargs in iterator:
                task: Task[R] = self._task_master.submit(func, *args, **kwargs)
                batch._tasks.append(task)

            return batch
        else:
            # Use direct execution (original behavior)
            if not self._process_pool:
                raise RuntimeError("TaskExecutor not initialized for direct mode")

            # Check if any function is a method (which can't be pickled)
            for func, _, _ in tasks:
                param_names = list(signature(func).parameters.keys())
                if param_names and param_names[0] in ("self", "cls"):
                    raise ValueError(
                        f"Cannot use multiprocess with instance or class method "
                        f"{func.__name__}. Methods with 'self' or 'cls' parameters "
                        "cannot be pickled for multiprocessing. Consider using a "
                        "standalone function or static method."
                    )
                if iscoroutinefunction(func):
                    raise ValueError(
                        f"Cannot use multiprocess with coroutine function "
                        f"{func.__name__}. Async functions are not supported with "
                        "multiprocessing."
                    )

            batch = Tasks[R]()
            iterator = tqdm(tasks, desc="Direct Processing") if show_progress else tasks

            for func, args, kwargs in iterator:
                start_time = perf_counter()
                future = self._process_pool.submit(func, *args, **kwargs)
                task = self._create_task(future, args, kwargs, start_time)
                batch._tasks.append(task)

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

        Note: Race method always uses direct thread pool execution for optimal
        performance and to avoid resource contention, regardless of executor mode.

        Args:
            tasks: List of (func, args, kwargs) tuples to race
            preferred_task_idx: If all tasks fail, use this task's result. If None,
                use the result of the task that finishes last.
            timeout: Maximum time to wait for a result

        Returns:
            Task: The winning task's result, or an empty task if shutting down.
        """
        if self._shutdown_event.is_set():
            self._log.warning("TaskExecutor is shutting down, returning None")
            return Task(Future())

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

        # Always use direct thread pool execution for race operations
        # This avoids resource contention and provides consistent, fast execution
        if not self._thread_pool:
            self._thread_pool = ThreadPoolExecutor(None, "task_executor")

        self._log.debug(f"🏃‍♂️ Racing {len(tasks)} tasks using Direct mode")
        for idx, (func, args, kwargs) in enumerate(tasks):
            task_start = perf_counter()
            future = self._thread_pool.submit(func, *args, **kwargs)
            task = self._create_task(future, args, kwargs, task_start)
            batch._tasks.append(task)

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
                    self._log.debug(f"🏆 Race won by task: {successful_tasks[0]}")
                    return successful_tasks[0]

                # If no successful tasks, check if we have a preferred task
                if preferred_task is not None:
                    if preferred_task._future in done:
                        self._log.debug(f"🎯 Using preferred task: {preferred_task}")
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
            if batch._tasks:
                return batch._tasks[0]
            else:
                # All tasks failed, return empty task
                return Task(Future())

        finally:
            # Cancel any remaining tasks
            for task in batch._tasks:
                if not task._future.done():
                    task._future.cancel()
                    self._active_tasks.discard(task)

    def cleanup(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        🧹 Clean up executor resources and shutdown thread/process pools.

        Args:
            wait: Whether to wait for running tasks to complete
            timeout: Maximum time to wait for tasks to complete (in seconds)
        """
        self._log.info("🧹 Cleaning up TaskExecutor resources...")
        self._shutdown_event.set()

        # Wait for active tasks to complete if requested
        if wait and self._active_tasks:
            self._log.info(
                f"⏳ Waiting for {len(self._active_tasks)} tasks to complete..."
            )
            start_time = time()
            while self._active_tasks and (
                timeout is None or (time() - start_time) < timeout
            ):
                # Remove completed tasks
                self._active_tasks = {
                    task for task in self._active_tasks if not task._future.done()
                }
                if self._active_tasks:
                    sleep(0.1)  # Small delay to prevent CPU spinning

            if self._active_tasks:
                self._log.warning(
                    f"⚠️ {len(self._active_tasks)} tasks did not complete in time"
                )

        # Shutdown the pools
        if self._thread_pool:
            self._thread_pool.shutdown(wait=False)
        if self._process_pool:
            self._process_pool.shutdown(wait=False)
        self._log.info("✅ TaskExecutor cleanup complete")

    @staticmethod
    def await_async[R](awaitable: Awaitable[R]) -> R:
        """
        ⌚ Run an async coroutine synchronously and wait for the result.

        Args:
            awaitable: The coroutine to execute

        Returns:
            The result of the coroutine
        """
        from asyncio import get_event_loop, new_event_loop, set_event_loop

        try:
            loop = get_event_loop()
        except RuntimeError as e:
            if "no current event loop" in str(e).lower():
                loop = new_event_loop()
                set_event_loop(loop)
            else:
                raise e
        return loop.run_until_complete(awaitable)

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
        try:
            self._log.info("Shutting down TaskExecutor Instance")
            self.cleanup()
        except Exception as e:
            self._log.error(f"Unable to shut down TaskExecutor Instance: {e}")


__all__: list[str] = ["TaskExecutor", "Task", "Tasks", "TaskMaster"]
