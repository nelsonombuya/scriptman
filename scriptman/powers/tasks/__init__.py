from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError, wait
from inspect import iscoroutinefunction
from threading import Lock, Thread, active_count
from time import perf_counter, sleep, time
from typing import Any, Awaitable, Callable, Optional

from loguru import Logger, logger
from tqdm import tqdm

from scriptman.core.config import config
from scriptman.powers.generics import P, R
from scriptman.powers.tasks._models import Task, Tasks


class TaskExecutor:
    """
    🧩 Task Executor

    Efficiently manages parallel task execution using threading with automatic
    resource monitoring and cleanup for high-load scenarios.

    Features:
    - Background execution with awaitable task futures ⏱️
    - Efficient parallel execution with thread pools ⚡
    - Elegant handling of errors with customizable exception behavior 🛡️
    - Comprehensive task monitoring with duration and status tracking 📊
    - Intelligent resource caps with idle-based cleanup 🎯
    - Named threads for better debugging and monitoring 🔍
    - Auto-batching for large workloads 📦
    - Singleton pattern for efficient resource sharing 🌐

    Examples:
        # Basic usage
        executor = TaskExecutor()
        task = executor.background(slow_function, arg1, arg2)
        result = task.await_result()

        # Batch processing with auto-batching
        batch = executor.multithread([
            (fetch_url, ("https://api1.com",), {}),
            (fetch_url, ("https://api2.com",), {"timeout": 30}),
        ])
        results = batch.await_results()

        # Race multiple tasks
        winner = executor.race([
            (fast_api_call, (), {}),
            (backup_api_call, (), {}),
        ])
        result = winner.await_result()

        # Context manager usage
        with TaskExecutor() as executor:
            task = executor.background(func)
            result = task.await_result()
    """

    # Singleton pattern
    __timeout: int = config.settings.get("tasks.idle_timeout", 30)
    __instance: Optional["TaskExecutor"] = None
    __max_workers: Optional[int] = None
    __resource_percentage: float = 50.0  # Default 50% of system resources
    __initialized: bool = False
    __is_shutdown: bool = False
    __lock: Lock = Lock()
    log: Logger = logger

    def __new__(cls, *args: Any, **kwargs: Any) -> "TaskExecutor":
        """🚀 Create or return singleton instance"""
        with cls.__lock:
            if cls.__instance is None:
                cls.__instance = super(TaskExecutor, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    @classmethod
    def set_resource_percentage(cls, percentage: float) -> None:
        """
        🎯 Dynamically set the resource percentage and recalculate worker limits.

        Args:
            percentage: Percentage of system resources to use (0.0-100.0)

        Examples:
            TaskExecutor.set_resource_percentage(75.0)  # Use 75% of system resources
            TaskExecutor.set_resource_percentage(25.0)  # Use 25% of system resources
        """
        if not 0.0 <= percentage <= 100.0:
            raise ValueError("Resource percentage must be between 0.0 and 100.0")

        cls.__resource_percentage = percentage

        # Recalculate resource caps
        (
            cpu_based_workers,
            memory_based_workers,
            resource_capped_workers,
        ) = cls.__calculate_resource_caps()

        logger.info(
            f"🎯 Updated resource caps ({percentage}%): CPU={cpu_based_workers}, "
            f"Memory={memory_based_workers}GB, Final={resource_capped_workers}"
        )

        # If instance exists, update its max_workers
        if cls.__instance and hasattr(cls.__instance, "executor"):
            cls.__instance.__update_worker_limit(resource_capped_workers)

    def __update_worker_limit(self, new_max_workers: int) -> None:
        """
        🔄 Dynamically update the worker limit and recreate the executor.

        Args:
            new_max_workers: New maximum number of workers
        """
        if self.__is_shutdown:
            return

        self.log.info(
            f"🔄 Updating worker limit from {self.__max_workers} to {new_max_workers}"
        )

        # Shutdown old executor
        old_executor = self.executor
        old_executor.shutdown(wait=False)

        # Update max_workers
        self.__max_workers = new_max_workers

        # Create new executor with updated limits
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=self.__max_workers,
            thread_name_prefix="TaskExecutor-",
        )

        logger.info(f"✅ Worker limit updated to {self.__max_workers}")

    @classmethod
    def __calculate_resource_caps(cls) -> tuple[int, int, int]:
        """
        🧮 Calculate resource caps based on current percentage setting.

        Returns:
            Tuple of (cpu_based_workers, memory_based_workers, resource_capped_workers)
        """
        from os import cpu_count as os_cpu_count

        from psutil import virtual_memory

        cpu_count = os_cpu_count() or 1
        total_memory_gb = virtual_memory().total / (1024**3)
        percentage = cls.__resource_percentage / 100

        cpu_based_workers = int(cpu_count * percentage)
        memory_based_workers = int(total_memory_gb * percentage)
        resource_capped_workers = min(cpu_based_workers, memory_based_workers)

        return cpu_based_workers, memory_based_workers, resource_capped_workers

    @classmethod
    def get_resource_percentage(cls) -> float:
        """
        📊 Get the current resource percentage setting.

        Returns:
            Current resource percentage (0.0-100.0)
        """
        return cls.__resource_percentage

    def __init__(self, max_workers: Optional[int] = None) -> None:
        """
        🚀 Initialize the TaskExecutor with resource monitoring.

        Args:
            max_workers: Maximum number of threads (default: resource-aware calculation)

        Note:
            Resource management caps at 50% of system resources (CPU/RAM).
            Idle threads are automatically cleaned up, so no thread count limits.
        """
        if self.__initialized:
            return

        # Calculate resource-capped workers using helper method
        (
            cpu_based_workers,
            memory_based_workers,
            resource_capped_workers,
        ) = self.__calculate_resource_caps()

        # Prefer user-provided max_workers if provided,
        # otherwise use resource-capped workers
        if max_workers is not None and max_workers > 0:
            self.__max_workers = max_workers
        else:
            self.__max_workers = resource_capped_workers

        logger.debug(
            f"🎯 Resource caps ({self.__class__.__resource_percentage}%): "
            f"CPU={cpu_based_workers}, "
            f"Memory={memory_based_workers}GB, "
            f"Resource-capped={resource_capped_workers}, "
            f"Max workers={self.__max_workers}"
        )

        # Create executor
        self.executor = ThreadPoolExecutor(
            max_workers=self.__max_workers,
            thread_name_prefix="TaskExecutor-",
        )

        # Monitoring
        self.__monitoring: bool = False
        self.__monitor_thread: Optional[Thread] = None
        self.__initial_thread_count: int = active_count()
        self.__last_activity_time: float = time()
        self.__start_monitoring()

        self.__initialized = True
        logger.info(f"🎯 TaskExecutor initialized with {self.__max_workers} workers")

    """
    Context manager entry and exit to cleanup the resources when the context is exited.
    """

    def __enter__(self) -> "TaskExecutor":
        """🚪 Context manager entry"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """🚪 Context manager exit"""
        self.cleanup()

    """
    Background monitoring thread to check if the resource usage is within the thresholds
    and if not, recycle the thread pool.
    """

    def __start_monitoring(self) -> None:
        """🔍 Start background monitoring thread"""
        if self.__monitoring:
            return

        self.__monitoring = True
        self.__monitor_thread = Thread(
            daemon=True,
            name="TaskExecutor-Monitor",
            target=self.__monitor_resources,
        )
        self.__monitor_thread.start()
        logger.debug("🔍 Started aggressive resource monitoring")

    def __monitor_resources(self) -> None:
        """🔍 Background thread monitoring for idle thread cleanup"""
        while self.__monitoring and not self.__is_shutdown:
            try:
                # 🧹 Check for idle threads (primary cleanup trigger)
                current_threads: int = active_count()
                is_idle = time() - self.__last_activity_time > self.__timeout

                if is_idle and current_threads > 0:
                    idle_duration = time() - self.__last_activity_time
                    logger.info(
                        f"🧹 Cleaning up idle threads (idle for {idle_duration:.1f}s, "
                        f"{current_threads} threads)"
                    )
                    self.__recycle_pool()

                sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.debug(f"⚠️ Monitoring error (ignored): {e}")
                sleep(5)

    def __recycle_pool(self) -> None:
        """🔄 Recycle the thread pool to free resources"""
        old_executor = self.executor
        old_executor.shutdown(wait=False)

        self.executor = ThreadPoolExecutor(
            max_workers=self.__max_workers,
            thread_name_prefix="TaskExecutor-",
        )

        logger.debug("🔄 Thread pool recycled")

    """
    API Methods for running tasks.
    background: 🚀 Run a single task in the background.
    multithread: 🌐 Process I/O-bound tasks in parallel using threading.
        Works well for a single method running in parallel.
    parallel: 🔄 Process I/O-bound tasks in parallel using multiprocessing.
        Works well for different methods running in parallel.
    race: 🏃‍♂️ Race multiple tasks and return the first successful result.
    """

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
            task = executor.background(slow_function, "arg1", kwarg=123)
            result = task.await_result()
        """
        if self.__is_shutdown:
            logger.warning("TaskExecutor is shutting down, returning empty Task")
            return Task[R](Future[R]())

        # Update activity time for idle thread management
        self.__last_activity_time = time()
        start_time = perf_counter()

        if iscoroutinefunction(func):
            future = self.executor.submit(self.await_async, func(*args, **kwargs))
        else:
            future = self.executor.submit(func, *args, **kwargs)

        return Task[R](future, start_time)

    def multithread(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool = True,
        auto_batch: bool = True,
        batch_size: int = 1000,
    ) -> Tasks[R]:
        """
        🌐 Process I/O-bound tasks in parallel using threading.

        Args:
            tasks: List of (func, args, kwargs) tuples
            show_progress: Whether to show a progress bar
            auto_batch: Whether to automatically batch large task lists
            batch_size: Size of batches for auto-batching

        Returns:
            Tasks: Container that manages all tasks together

        Examples:
            batch = executor.multithread([
                (fetch_url, ("https://api1.com",), {}),
                (fetch_url, ("https://api2.com",), {"timeout": 30}),
            ])
            results = batch.await_results()
        """
        if self.__is_shutdown:
            logger.warning("TaskExecutor is shutting down, returning empty Tasks")
            return Tasks[R]()

        if not tasks:
            raise ValueError("Tasks list cannot be empty")

        if auto_batch and len(tasks) > batch_size:
            return self.__process_batched(tasks, batch_size, show_progress)

        return self.__execute_batch(tasks, show_progress)

    def parallel(
        self,
        tasks: list[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool = True,
        auto_batch: bool = True,
        batch_size: int = 1000,
    ) -> Tasks[Any]:
        """
        🔄 Process tasks in parallel using threading.

        Args:
            tasks: List of (func, args, kwargs) tuples
            show_progress: Whether to show a progress bar
            auto_batch: Whether to automatically batch large task lists
            batch_size: Size of batches for auto-batching

        Returns:
            Tasks: Container that manages all tasks together
        """
        return self.multithread(
            show_progress=show_progress,
            auto_batch=auto_batch,
            batch_size=batch_size,
            tasks=tasks,
        )

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
            preferred_task_idx: If all tasks fail, use this task's result
            timeout: Maximum time to wait for a result

        Returns:
            Task: The winning task's result

        Examples:
            winner = executor.race([
                (fast_api_call, (), {}),
                (backup_api_call, (), {}),
            ])
            result = winner.await_result()
        """
        if self.__is_shutdown:
            logger.warning("TaskExecutor is shutting down, returning empty Task")
            return Task(Future())

        if not tasks:
            raise ValueError("Tasks list cannot be empty")

        # Submit all tasks
        task_objects = []
        for func, args, kwargs in tasks:
            task = self.background(func, *args, **kwargs)
            task_objects.append(task)

        # Wait for first completion
        futures = [task.future for task in task_objects]
        done, not_done = wait(futures, timeout=timeout, return_when="FIRST_COMPLETED")

        if not done:
            # Timeout occurred
            if timeout:
                raise TimeoutError(f"No task completed within {timeout} seconds")
            return Task(Future())

        # Find the first successful task
        for task in task_objects:
            if task.future in done and task.is_successful:
                return task

        # If no successful task, use preferred or last completed
        if preferred_task_idx is not None and 0 <= preferred_task_idx < len(task_objects):
            return task_objects[preferred_task_idx]

        # Return the first completed task (even if failed)
        for task in task_objects:
            if task.future in done:
                return task

        return Task(Future())

    def __process_batched(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        batch_size: int,
        show_progress: bool,
    ) -> Tasks[R]:
        """Process tasks in batches for large workloads"""
        all_tasks = Tasks[R]()
        total_batches = (len(tasks) + batch_size - 1) // batch_size

        iterator = (
            tqdm(
                range(0, len(tasks), batch_size),
                desc="Processing batches",
                total=total_batches,
            )
            if show_progress
            else range(0, len(tasks), batch_size)
        )

        for i in iterator:
            batch_tasks = tasks[i : i + batch_size]
            batch = self.__execute_batch(batch_tasks, show_progress=False)
            all_tasks._tasks.extend(batch._tasks)

        return all_tasks

    def __execute_batch(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool,
    ) -> Tasks[R]:
        """⚙️ Execute a batch of tasks"""
        batch = Tasks[R]()
        iterator = tqdm(tasks, desc="Executing tasks") if show_progress else tasks

        for func, args, kwargs in iterator:
            task = self.background(func, *args, **kwargs)
            batch._tasks.append(task)

        return batch

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

    def cleanup(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        🧹 Clean up resources and shutdown the executor.

        Args:
            wait: Whether to wait for running tasks to complete
            timeout: Maximum time to wait for shutdown
        """
        if self.__is_shutdown:
            return

        self.__is_shutdown = True
        self.__monitoring = False

        if self.__monitor_thread and self.__monitor_thread.is_alive():
            self.__monitor_thread.join(timeout=timeout or 1.0)

        try:
            self.executor.shutdown(wait=wait)
            logger.info("✅ TaskExecutor cleanup completed")
        except Exception as e:
            logger.warning(f"⚠️ Error during cleanup: {e}")

    def __del__(self) -> None:
        """Ensure cleanup during garbage collection"""
        if not self.__is_shutdown:
            try:
                # Use non-blocking cleanup during garbage collection
                self.cleanup(wait=False)
            except Exception:
                # Silently handle exceptions during garbage collection
                # Logging during __del__ can cause issues
                pass


__all__: list[str] = ["TaskExecutor", "Task", "Tasks"]
