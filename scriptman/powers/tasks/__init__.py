from concurrent.futures import Future, TimeoutError, wait
from inspect import iscoroutinefunction
from threading import Lock, Thread
from time import perf_counter, sleep
from typing import Any, Awaitable, Callable, Optional

from loguru import Logger, logger
from tqdm import tqdm

from scriptman.core.config import config
from scriptman.powers.generics import P, R
from scriptman.powers.tasks._execution_manager import ExecutionManager
from scriptman.powers.tasks._models import Task, Tasks
from scriptman.powers.tasks._thread_executor import ThreadExecutor


class TaskManager:
    """
    🧩 Task Manager

    Unified task execution manager with pluggable execution backends.
    Provides clean APIs for different execution strategies with global
    resource management and monitoring.

    Features:
    - Pluggable execution backends (threading, multiprocessing, async) 🔧
    - Global resource management and monitoring 📊
    - Lazy initialization of executors ⚡
    - Thread-safe singleton pattern 🔒
    - Automatic cleanup of idle executors 🧹
    - Unified API across execution types 🎯

    Examples:
        # Basic usage
        manager = TaskManager()
        task = manager.threads.background(slow_function, arg1, arg2)
        result = task.await_result()

        # Different execution strategies
        io_task = manager.threads.background(fetch_url, url)
        cpu_task = manager.process.background(heavy_compute, data)  # Future
        async_task = manager.asynchronous.background(stream_data, endpoint)  # Future

        # Batch processing
        batch = manager.multithread([
            (fetch_url, ("https://api1.com",), {}),
            (fetch_url, ("https://api2.com",), {"timeout": 30}),
        ])
        results = batch.await_results()

    Quick Reference:
        manager = TaskManager()                           # Create manager (singleton)
        task = manager.background(func, *args)            # Single background task
        batch = manager.multithread(tasks)                # Parallel tasks
        winner = manager.race(tasks)                      # Race tasks
        result = task.await_result()                      # Get result
        results = batch.await_results()                   # Get all results
        manager.cleanup_all()                             # Manual cleanup (optional)

        # Access specific executors
        manager.threads                                   # Thread-based executor
        manager.process                                   # Process-based (future)
        manager.asynchronous                              # Async-based (future)

    Future Enhancements (TODO):
        - Task Progress Tracking: Track progress of long-running tasks
        - Task Cancellation: Ability to cancel running tasks
        - Task Dependencies: Support task dependencies (Task A waits for Task B)
        - Metrics & Statistics: Track execution times, success rates, etc.
    """

    # Singleton pattern
    __instance: Optional["TaskManager"] = None
    __is_shutdown: bool = False
    __initialized: bool = False
    __lock: Lock = Lock()

    # Global configuration
    __idle_timeout: int = config.settings.get("tasks.idle_timeout", 30)
    __resource_percentage: float = config.settings.get("tasks.resource_percentage", 50.0)

    log: Logger = logger

    def __new__(cls, *args: Any, **kwargs: Any) -> "TaskManager":
        """🚀 Create or return singleton instance"""
        with cls.__lock:
            if cls.__instance is None:
                cls.__instance = super(TaskManager, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    def __init__(self) -> None:
        """🚀 Initialize the TaskManager"""
        if self.__initialized:
            return

        # Lazy initialization - only create when accessed
        self.__thread_executor: Optional[ThreadExecutor] = None
        self.__process_executor: Optional[ExecutionManager] = None
        self.__asynchronous_executor: Optional[ExecutionManager] = None

        # Global monitoring
        self.__global_monitor_thread: Optional[Thread] = None
        self.__executors_lock: Lock = Lock()
        self.__monitoring: bool = False

        self.__initialized = True
        logger.info("🎯 TaskManager initialized")

    @property
    def threads(self) -> ThreadExecutor:
        """⛓️ Get thread executor (lazy initialization)"""
        with self.__executors_lock:
            if self.__thread_executor is None:
                self.__thread_executor = ThreadExecutor(
                    max_workers=self.__calculate_thread_workers(),
                    idle_timeout=self.__idle_timeout,
                )
                self.__start_global_monitoring()
            return self.__thread_executor

    @property
    def process(self) -> ExecutionManager:
        """🔧 Get process executor (lazy initialization) - Future implementation"""
        # TODO: Implement ProcessExecutor
        raise NotImplementedError("ProcessExecutor not yet implemented")

    @property
    def asynchronous(self) -> ExecutionManager:
        """🔧 Get asynchronous executor (lazy initialization) - Future implementation"""
        # TODO: Implement AsynchronousExecutor
        raise NotImplementedError("AsynchronousExecutor not yet implemented")

    def __start_global_monitoring(self) -> None:
        """🔍 Start single monitoring thread for all executors"""
        if self.__monitoring:
            return

        try:
            self.__monitoring = True
            self.__global_monitor_thread = Thread(
                daemon=True,
                name="TaskManager-GlobalMonitor",
                target=self.__monitor_all_executors,
            )
            self.__global_monitor_thread.start()
            logger.debug("🔍 Started global monitoring")
        except Exception as e:
            logger.warning(f"⚠️ Could not start monitoring: {e}")
            # Continue without monitoring - better than crashing

    def __monitor_all_executors(self) -> None:
        """🔍 Single monitoring thread that rules them all"""
        while self.__monitoring and not self.__is_shutdown:
            try:
                # Check each executor for idle state
                with self.__executors_lock:
                    if self.__thread_executor and self.__thread_executor.is_idle:
                        logger.info("🧹 threads executor idle, cleaning up")
                        self.__thread_executor.cleanup()
                        self.__thread_executor = None

                    # TODO: Check other executors when implemented
                    # if self.__process_executor and self.__process_executor.is_idle():
                    #     logger.info("🧹 process executor idle, cleaning up")
                    #     self.__process_executor.cleanup()
                    #     self.__process_executor = None

                    # if self.__asynchronous_executor
                    # and self.__asynchronous_executor.is_idle():
                    #     logger.info("🧹 asynchronous executor idle, cleaning up")
                    #     self.__asynchronous_executor.cleanup()
                    #     self.__asynchronous_executor = None

                    # Stop monitoring if no executors
                    if not any(
                        [
                            self.__thread_executor,
                            self.__process_executor,
                            self.__asynchronous_executor,
                        ]
                    ):
                        self.__monitoring = False
                        break

                sleep(5)
            except Exception as e:
                logger.debug(f"⚠️ Global monitoring error (ignored): {e}")
                sleep(5)

    @classmethod
    def set_resource_percentage(cls, percentage: float) -> None:
        """
        🎯 Set global resource percentage with validation

        Args:
            percentage: Percentage of system resources to use (0.0-100.0)

        Examples:
            TaskManager.set_resource_percentage(75.0)  # Use 75% of system resources
            TaskManager.set_resource_percentage(25.0)  # Use 25% of system resources
        """
        assert isinstance(
            percentage, (int, float)
        ), "Resource percentage must be a number"
        assert (
            0.0 <= percentage <= 100.0
        ), "Resource percentage must be between 0.0 and 100.0"

        cls.__resource_percentage = percentage

        # Update existing executors
        if cls.__instance:
            with cls.__instance.__executors_lock:
                if cls.__instance.__thread_executor:
                    cls.__instance.__thread_executor.update_resource_limits(
                        cls.__instance.__calculate_thread_workers()
                    )
                # TODO: Update other executors when implemented

        logger.info(f"🎯 Updated resource percentage to {percentage}%")

    @classmethod
    def get_resource_percentage(cls) -> float:
        """🔍 Get the current resource percentage setting"""
        return cls.__resource_percentage

    def __calculate_thread_workers(self) -> int:
        """🔍 Calculate optimal thread workers based on system resources"""
        try:
            from os import cpu_count

            from psutil import virtual_memory

            cpu_count_val = cpu_count() or 1
            total_memory_gb = virtual_memory().total / (1024**3)
            percentage = self.__resource_percentage / 100

            cpu_based_workers = int(cpu_count_val * percentage)
            memory_based_workers = int(total_memory_gb * percentage)
            resource_capped_workers = min(cpu_based_workers, memory_based_workers)

            return max(1, resource_capped_workers)
        except Exception as e:
            logger.warning(f"⚠️ Could not calculate thread workers: {e}")
            return 4  # Safe default

    # Context manager support
    def __enter__(self) -> "TaskManager":
        """🚪 Context manager entry"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """🚪 Context manager exit"""
        self.cleanup_all()

    # API Methods - Maintain backward compatibility
    def background(self, func: Callable[P, R], *args: Any, **kwargs: Any) -> Task[R]:
        """
        🚀 Run a single task in the background (using threads)

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Task: Container that can be awaited to get the result

        Examples:
            task = manager.background(slow_function, "arg1", kwarg=123)
            result = task.await_result()
        """
        if self.__is_shutdown:
            logger.warning("TaskManager is shutting down, returning empty Task")
            return Task[R](Future[R]())

        future: Future[R]
        start_time = perf_counter()

        if iscoroutinefunction(func):
            future = self.threads.submit_task(self.await_async, func(*args, **kwargs))
        else:
            future = self.threads.submit_task(func, *args, **kwargs)

        return Task[R](future, start_time)

    def multithread(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool = True,
    ) -> Tasks[R]:
        """
        🌐 Process I/O-bound tasks in parallel using threading

        Args:
            tasks: List of (func, args, kwargs) tuples
            show_progress: Whether to show a progress bar

        Returns:
            Tasks: Container that manages all tasks together

        Examples:
            batch = manager.multithread([
                (fetch_url, ("https://api1.com",), {}),
                (fetch_url, ("https://api2.com",), {"timeout": 30}),
            ])
            results = batch.await_results()
        """
        if self.__is_shutdown:
            logger.warning("TaskManager is shutting down, returning empty Tasks")
            return Tasks[R]()

        if not tasks:
            raise ValueError("Tasks list cannot be empty")

        batch = Tasks[R]()
        iterator = tqdm(tasks, desc="Executing tasks") if show_progress else tasks

        for func, args, kwargs in iterator:
            task = self.background(func, *args, **kwargs)
            batch._tasks.append(task)

        return batch

    def parallel(
        self,
        tasks: list[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]],
        show_progress: bool = True,
    ) -> Tasks[Any]:
        """
        🔄 Process tasks in parallel using threading (alias for multithread)

        Args:
            tasks: List of (func, args, kwargs) tuples
            show_progress: Whether to show a progress bar

        Returns:
            Tasks: Container that manages all tasks together
        """
        return self.multithread(show_progress=show_progress, tasks=tasks)

    def race(
        self,
        tasks: list[tuple[Callable[P, R], tuple[Any, ...], dict[str, Any]]],
        *,
        preferred_task_idx: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> Task[R]:
        """
        🏃‍♂️ Race multiple tasks and return the first successful result

        Args:
            tasks: List of (func, args, kwargs) tuples to race
            preferred_task_idx: If all tasks fail, use this task's result
            timeout: Maximum time to wait for a result

        Returns:
            Task: The winning task's result

        Examples:
            winner = manager.race([
                (fast_api_call, (), {}),
                (backup_api_call, (), {}),
            ])
            result = winner.await_result()
        """
        if self.__is_shutdown:
            logger.warning("TaskManager is shutting down, returning empty Task")
            return Task(Future())

        if not tasks:
            raise ValueError("Tasks list cannot be empty")

        # Submit all tasks using threads
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

    @staticmethod
    def await_async[R](awaitable: Awaitable[R]) -> R:
        """
        ⌚ Run an async coroutine synchronously and wait for the result

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

    def cleanup_all(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        🧹 Clean up all executors

        Args:
            wait: Whether to wait for running tasks to complete
            timeout: Maximum time to wait for shutdown
        """
        if self.__is_shutdown:
            return

        self.__is_shutdown = True
        self.__monitoring = False

        # Join monitoring thread
        if self.__global_monitor_thread and self.__global_monitor_thread.is_alive():
            self.__global_monitor_thread.join(timeout=timeout or 1.0)

        # Cleanup all executors
        with self.__executors_lock:
            if self.__thread_executor:
                self.__thread_executor.cleanup(wait=wait, timeout=timeout)
                self.__thread_executor = None

            # TODO: Cleanup other executors when implemented
            # if self.__process_executor:
            #     self.__process_executor.cleanup(wait=wait, timeout=timeout)
            #     self.__process_executor = None

        logger.info("✅ TaskManager cleanup completed")

    def get_global_status(self) -> dict[str, Any]:
        """🔍 Get comprehensive status of all executors"""
        try:
            return {
                "monitoring_active": self.__monitoring,
                "threads": self.__get_executor_status(self.__thread_executor),
                "process": self.__get_executor_status(self.__process_executor),
                "asynchronous": self.__get_executor_status(self.__asynchronous_executor),
            }
        except Exception as e:
            logger.error(f"❌ Error getting status: {e}")
            return {"error": str(e)}

    def __get_executor_status(
        self, executor: Optional[ExecutionManager]
    ) -> dict[str, Any]:
        """🔍 Get status of a specific executor"""
        if executor is None:
            return {"active": False, "idle": True, "workers": 0, "active_tasks": 0}

        return {
            "active": True,
            "idle": executor.is_idle,
            "workers": getattr(executor, "_max_workers", 0),
            "active_tasks": getattr(executor, "_ThreadExecutor__active_task_count", 0),
        }

    def __del__(self) -> None:
        """🗑️ Ensure cleanup during garbage collection"""
        if not self.__is_shutdown:
            try:
                # Use non-blocking cleanup during garbage collection
                self.cleanup_all(wait=False)
            except Exception:
                # Silently handle exceptions during garbage collection
                # Logging during __del__ can cause issues
                pass


__all__: list[str] = ["TaskManager", "Task", "Tasks"]
