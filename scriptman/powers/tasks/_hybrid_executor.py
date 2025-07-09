from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from inspect import iscoroutinefunction
from pickle import PicklingError, dumps
from threading import RLock
from time import time
from typing import Any, Optional

from loguru import logger

from scriptman.powers.generics import AsyncFunc, Func, P, R
from scriptman.powers.tasks._models import TaskSubmission


class HybridExecutor:
    """⚡ Hybrid executor managing both thread and process pools within a single process"""

    def __init__(self, max_threads: int = 8, max_processes: int = 2):
        assert max_threads > 0, "max_threads must be greater than 0"
        assert max_processes >= 0, "max_processes must be greater than or equal to 0"

        # Initialize thread and process pools
        self.max_threads: int = max_threads
        self.max_processes: int = max_processes
        self.thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            thread_name_prefix="hybrid",
            max_workers=max_threads,
        )
        self.process_pool: Optional[ProcessPoolExecutor] = (
            ProcessPoolExecutor(max_workers=max_processes)
            if self.max_processes > 0
            else None
        )

        # Initialize lock for thread safety
        self._lock: RLock = RLock()

        # Initialize active tasks and last activity time
        self.active_tasks: set[str] = set()  # Set of active task IDs
        self.last_activity: float = time()  # Time of last activity

    def submit(self, task_submission: TaskSubmission) -> Future[Any]:
        """🚀 Submit task to appropriate executor based on task type"""
        with self._lock:
            self.active_tasks.add(task_submission.task_id)
            self.last_activity = time()  # Update last activity time

            # Handle async functions
            if iscoroutinefunction(task_submission.func):
                # Always use thread pool for async functions
                future = self.thread_pool.submit(
                    self._run_async,
                    task_submission.func,
                    *task_submission.args,
                    **task_submission.kwargs,
                )
            else:
                # Choose executor based on task type for sync functions
                if task_submission.task_type == "cpu" and self.process_pool:
                    # CPU-intensive tasks prefer processes
                    if self._can_use_processes(task_submission.func):
                        future = self.process_pool.submit(
                            task_submission.func,
                            *task_submission.args,
                            **task_submission.kwargs,
                        )
                    else:
                        # Fallback to threads if function can't be pickled
                        future = self.thread_pool.submit(
                            task_submission.func,
                            *task_submission.args,
                            **task_submission.kwargs,
                        )
                else:
                    # I/O and mixed tasks use threads
                    future = self.thread_pool.submit(
                        task_submission.func,
                        *task_submission.args,
                        **task_submission.kwargs,
                    )

            # Add completion callback
            future.add_done_callback(
                lambda f: self._task_completed(task_submission.task_id)
            )
            return future

    def _run_async(self, func: AsyncFunc[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """Run an async function in a thread"""
        from asyncio import get_event_loop, new_event_loop, set_event_loop

        try:
            loop = get_event_loop()
        except RuntimeError as e:
            if "no current event loop" in str(e).lower():
                loop = new_event_loop()
                set_event_loop(loop)
            else:
                raise e
        return loop.run_until_complete(func(*args, **kwargs))

    def _can_use_processes(self, func: Func[P, R]) -> bool:
        """🔍 Check if function can be used with multiprocessing"""
        try:
            dumps(func)
            return True
        except (PicklingError, AttributeError):
            return False

    def _task_completed(self, task_id: str) -> None:
        """✅ Handle task completion"""
        with self._lock:
            self.active_tasks.discard(task_id)
            self.last_activity = time()  # Update last activity time

    def get_load(self) -> float:
        """📊 Get current load metric (0.0 - 1.0)"""
        with self._lock:
            thread_load = (
                len([f for f in self.thread_pool._threads if f.is_alive()])
                / self.max_threads
            )
            process_load = 0.0
            if self.process_pool:
                # Estimate process load based on active tasks
                process_load = len(self.active_tasks) / max(self.max_processes, 1)
            return max(thread_load, process_load)

    def is_idle(self, idle_threshold: float = 120.0) -> bool:
        """💤 Check if executor has been idle for threshold seconds"""
        with self._lock:
            return (time() - self.last_activity) > idle_threshold and len(
                self.active_tasks
            ) == 0

    def shutdown(self, wait: bool = True) -> None:
        """🛑 Shutdown executor pools"""
        logger.debug("Shutting down HybridExecutor")
        self.thread_pool.shutdown(wait=wait)
        if self.process_pool:
            self.process_pool.shutdown(wait=wait)
