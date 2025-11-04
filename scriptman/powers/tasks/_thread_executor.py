try:
    from concurrent.futures import Future, ThreadPoolExecutor
    from inspect import iscoroutinefunction
    from threading import Lock
    from time import time
    from typing import Any, Awaitable, Optional, cast

    from loguru import logger

    from scriptman.powers.generics import Func, P, R
    from scriptman.powers.tasks._execution_manager import ExecutionManager
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[tasks]"
    )


class ThreadExecutor(ExecutionManager):
    """
    🧵 Thread-based task execution

    Efficiently manages parallel task execution using threading with automatic
    resource monitoring and cleanup for high-load scenarios.

    Features:
    - Lazy initialization of thread pools ⚡
    - Thread-safe operations with proper locking 🔒
    - Comprehensive exception handling 🛡️
    - Resource cleanup in __del__ 🧹
    - Activity tracking for idle detection 📊
    """

    def __init__(self, max_workers: Optional[int] = None, idle_timeout: int = 30) -> None:
        """
        🚀 Initialize the ThreadExecutor

        Args:
            max_workers: Maximum number of threads (default: resource-aware calculation)
            idle_timeout: Time in seconds before considering executor idle
        """
        # Private state variables
        self.__executor: Optional[ThreadPoolExecutor] = None
        self.__last_activity_time: float = time()
        self.__is_shutdown: bool = False
        self.__active_task_count: int = 0

        # Configuration
        self._max_workers = max_workers or self.__calculate_optimal_workers()
        self._idle_timeout = idle_timeout

        # Thread safety
        self.__lock: Lock = Lock()
        logger.debug(f"🧵 ThreadExecutor initialized with {self._max_workers} workers")

    def submit_task(
        self, func: Func[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> Future[R]:
        """
        📤 Submit a task to the thread pool

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Future: The future object representing the task

        Raises:
            RuntimeError: If the executor is shutdown
        """
        if self.__is_shutdown:
            raise RuntimeError("ThreadExecutor is shutdown")

        with self.__lock:
            self.__ensure_executor()
            self.__last_activity_time = time()
            self.__active_task_count += 1

        logger.debug(f"📤 Submitted task: {func.__name__}")

        try:
            if self.__executor is None:
                raise RuntimeError("Executor not initialized")

            if iscoroutinefunction(func):
                future = self.__executor.submit(self.await_async, func(*args, **kwargs))
            else:
                future = self.__executor.submit(func, *args, **kwargs)

            def _on_done(_f: Future[Any]) -> None:
                with self.__lock:
                    self.__active_task_count -= 1
                    self.__last_activity_time = time()

            future.add_done_callback(_on_done)
            return cast(Future[R], future)
        except Exception as e:
            with self.__lock:
                self.__active_task_count -= 1
            logger.error(f"❌ Failed to submit task: {e}")
            raise

    def cleanup(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        🧹 Clean up resources

        Args:
            wait: Whether to wait for running tasks to complete
            timeout: Maximum time to wait for shutdown
        """
        if self.__is_shutdown:
            return

        self.__is_shutdown = True

        with self.__lock:
            if self.__executor:
                try:
                    self.__executor.shutdown(wait=wait)
                    logger.info("✅ ThreadExecutor cleanup completed")
                except Exception as e:
                    logger.warning(f"⚠️ Error during cleanup: {e}")
                finally:
                    self.__executor = None

    @property
    def is_shutdown(self) -> bool:
        """🔍 Check if manager is shutdown"""
        return self.__is_shutdown

    @property
    def is_idle(self) -> bool:
        """🔍 Check if executor has been idle"""
        if self.__is_shutdown:
            return True
        if self.__active_task_count > 0:
            return False
        return time() - self.__last_activity_time > self._idle_timeout

    @property
    def last_activity_time(self) -> float:
        """🔍 Get last activity timestamp"""
        return self.__last_activity_time

    def update_resource_limits(self, max_workers: int) -> None:
        """
        🔄 Update worker limits

        Args:
            max_workers: New maximum number of workers
        """
        if self.__is_shutdown:
            return

        logger.info(f"🔄 Updating worker limit from {self._max_workers} to {max_workers}")

        with self.__lock:
            # Shutdown old executor
            if self.__executor:
                old_executor = self.__executor
                old_executor.shutdown(wait=False)

            # Update max_workers
            self._max_workers = max_workers

            # Create new executor with updated limits
            self.__executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="ThreadExecutor-",
            )

        logger.info(f"✅ Worker limit updated to {self._max_workers}")

    def __ensure_executor(self) -> None:
        """🔧 Ensure executor exists with proper state validation"""
        if self.__is_shutdown:
            raise RuntimeError("Cannot create executor: ThreadExecutor is shutdown")

        if self.__executor is None or self.__executor._shutdown:
            logger.debug("🔧 Creating new ThreadPoolExecutor")
            self.__executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="ThreadExecutor-",
            )

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

    def __calculate_optimal_workers(self) -> int:
        """
        🧮 Calculate optimal worker count with system awareness

        Returns:
            int: Optimal number of workers
        """
        try:
            from os import cpu_count

            from psutil import virtual_memory

            cpu_count_val = cpu_count() or 1
            memory_gb = virtual_memory().total / (1024**3)

            # Conservative calculation
            cpu_based = max(1, int(cpu_count_val * 0.5))
            memory_based = max(1, int(memory_gb * 0.5))

            return min(cpu_based, memory_based, 20)  # Cap at 20 threads
        except Exception as e:
            logger.warning(f"⚠️ Could not calculate optimal workers: {e}")
            return 4  # Safe default

    @property
    def info(self) -> dict[str, Any]:
        """🐛 Get comprehensive debug information"""
        return {
            "is_shutdown": self.__is_shutdown,
            "last_activity": self.__last_activity_time,
            "idle_duration": time() - self.__last_activity_time,
            "active_tasks": self.__active_task_count,
            "max_workers": self._max_workers,
            "executor_exists": self.__executor is not None,
            "executor_shutdown": self.__executor._shutdown if self.__executor else None,
        }

    def __del__(self) -> None:
        """🗑️ Ensure cleanup during garbage collection"""
        if not self.__is_shutdown:
            try:
                # Use non-blocking cleanup during garbage collection
                self.cleanup(wait=False)
            except Exception:
                # Silently handle exceptions during garbage collection
                # Logging during __del__ can cause issues
                pass
