"""
🎯 TaskMaster - Intelligent Task Management System

This module provides the TaskMaster singleton class that offers aggressive, intelligent
task management with hybrid multiprocessing and multithreading for maximum throughput
and resource utilization.

Key Features:
- Singleton TaskMaster with thread-safe and process-safe design
- Hybrid execution using both processes and threads aggressively
- Dynamic resource scaling with eager allocation
- Cache integration for automatic result storage and retrieval
- Unified FIFO priority queue with promotion capability
- Automatic cleanup of idle resources
- Production-ready with comprehensive error handling
"""

import uuid
from atexit import register
from concurrent.futures import Future
from queue import Empty, PriorityQueue
from threading import RLock, Thread
from time import sleep, time
from typing import Any, Literal, Optional

from loguru import logger

from scriptman.powers.cache import CacheManager
from scriptman.powers.generics import Func, P, R
from scriptman.powers.tasks._hybrid_executor import HybridExecutor
from scriptman.powers.tasks._models import Task, TaskException, TaskSubmission
from scriptman.powers.tasks._pool_manager import DynamicPoolManager
from scriptman.powers.tasks._resource_monitor import ResourceMonitor


class TaskMaster:
    """🎯 Singleton TaskMaster for intelligent task management"""

    __instance: Optional["TaskMaster"] = None
    __lock: RLock = RLock()

    def __new__(cls, *args: Any, **kwargs: Any) -> "TaskMaster":
        """🔍 Create a new TaskMaster instance"""
        if cls.__instance is None:
            with cls.__lock:
                if cls.__instance is None:
                    cls.__instance = super().__new__(cls, *args, **kwargs)
        return cls.__instance

    def __init__(self) -> None:
        """🔍 Initialize the TaskMaster"""
        if hasattr(self, "_initialized"):
            return

        # Core components
        self.pool_manager = DynamicPoolManager()
        self.resource_monitor = ResourceMonitor()
        self.cache_manager = CacheManager.get_instance()

        # Task management
        self.active_tasks: dict[str, Task[Any]] = {}
        self.pending_submissions: dict[str, TaskSubmission] = {}
        self.task_queue: PriorityQueue[TaskSubmission] = PriorityQueue()

        # Memory cache for non-picklable objects
        self._memory_cache: dict[str, Any] = {}

        # Thread safety
        self._running: bool = True
        self._task_lock: RLock = RLock()

        # Worker threads
        self._dispatcher_thread: Thread = Thread(target=self._dispatch_loop, daemon=True)
        self._dispatcher_thread.start()

        # Start monitoring
        self.resource_monitor.start_monitoring()

        # Register cleanup
        register(self.shutdown)

        self._initialized = True
        logger.info("🎯 TaskMaster initialized")

    @classmethod
    def get_instance(cls) -> "TaskMaster":
        """🔍 Get the TaskMaster instance"""
        if cls.__instance is None:
            with cls.__lock:
                if cls.__instance is None:
                    cls.__instance = TaskMaster()
        return cls.__instance

    def submit(
        self,
        func: Func[P, R],
        task_type: Literal["cpu", "io", "mixed"] = "mixed",
        priority: int = 0,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[R]:
        """🚀 Submit a task for execution"""
        if not self._running:
            logger.error("TaskMaster is shutting down, cannot submit tasks")
            raise RuntimeError("TaskMaster is shutting down, cannot submit tasks")

        # Generate unique task ID
        task_id: str = str(uuid.uuid4())

        # Create task submission
        submission = TaskSubmission(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            task_type=task_type,
            priority=priority,
        )

        # Create future and task with task_id
        future: Future[R] = Future()
        task = Task(_future=future, _task_id=task_id, _args=args, _kwargs=kwargs)

        with self._task_lock:
            self.pending_submissions[task_id] = submission
            self.active_tasks[task_id] = task

        # Queue for processing
        self.task_queue.put(submission)

        logger.debug(
            f"📥 Submitted task {task_id[:8]} (type={task_type}, priority={priority})"
        )
        return task

    def promote_task(self, task_id: str) -> None:
        """⚡ Promote a task to foreground for priority processing"""
        with self._task_lock:
            if task_id in self.pending_submissions:
                submission = self.pending_submissions[task_id]
                if not submission.promoted:
                    submission.promoted = True
                    # Re-queue with promotion
                    self.task_queue.put(submission)
                    logger.debug(f"⚡ Promoted task {task_id[:8]} to foreground")

    def _dispatch_loop(self) -> None:
        """🔄 Main dispatch loop for processing tasks"""
        while self._running:
            try:
                # Get next task with timeout to allow shutdown
                try:
                    submission = self.task_queue.get(timeout=1.0)
                except Empty:
                    continue

                # Skip if task was already processed
                if submission.task_id not in self.pending_submissions:
                    self.task_queue.task_done()
                    continue

                # Get executor and submit task
                executor = self.pool_manager.get_available_executor()
                future = executor.submit(submission)

                # Update task future
                with self._task_lock:
                    if submission.task_id in self.active_tasks:
                        task = self.active_tasks[submission.task_id]
                        # Transfer result from executor future to task future
                        self._bridge_futures(future, task._future, submission.task_id)
                        # Remove from pending
                        self.pending_submissions.pop(submission.task_id, None)

                self.task_queue.task_done()

            except Exception as e:
                logger.error(f"Dispatch loop error: {e}")

    def _bridge_futures(self, source: Future[R], target: Future[R], task_id: str) -> None:
        """🌉 Bridge results from source future to target future"""

        def bridge_result(src_future: Future[R]) -> None:
            try:
                if src_future.cancelled():
                    target.cancel()
                elif src_future.exception():
                    if isinstance(exception := src_future.exception(), Exception) and (
                        task := self.active_tasks.get(task_id)
                    ):
                        task._cache_result(TaskException(exception))
                    target.set_exception(exception)
                else:
                    result = src_future.result()
                    if task := self.active_tasks.get(task_id):
                        task._cache_result(result)
                    target.set_result(result)
            except Exception as e:
                target.set_exception(e)
            finally:
                # Cleanup
                with self._task_lock:
                    self.active_tasks.pop(task_id, None)

        source.add_done_callback(bridge_result)

    def get_stats(self) -> dict[str, Any]:
        """📊 Get TaskMaster statistics"""
        with self._task_lock:
            active_count: int = len(self.active_tasks)
            pending_count: int = len(self.pending_submissions)
            memory_cache_count: int = len(self._memory_cache)

        return {
            "pending_tasks": pending_count,
            "active_tasks": active_count,
            "executors": len(self.pool_manager.executors),
            "memory_cache_size": memory_cache_count,
            "cpu_load": self.resource_monitor.get_cpu_load(),
            "memory_load": self.resource_monitor.get_memory_load(),
            "system_load": self.resource_monitor.get_system_load(),
        }

    def clear_memory_cache(self) -> None:
        """🧹 Clear the in-memory cache"""
        with self._task_lock:
            cache_size = len(self._memory_cache)
            self._memory_cache.clear()
            logger.info(f"🧹 Cleared {cache_size} items from memory cache")

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """🛑 Shutdown TaskMaster and cleanup resources"""
        if not self._running:
            return

        logger.info("🛑 Shutting down TaskMaster...")
        self._running = False

        # Stop accepting new tasks
        start_time: float = time()

        # Wait for pending tasks if requested
        if wait:
            while self.pending_submissions or self.active_tasks:
                if timeout and (time() - start_time) > timeout:
                    logger.warning("⚠️ Shutdown timeout reached, forcing shutdown")
                    break
                sleep(0.1)

        # Cleanup components
        self.resource_monitor.stop_monitoring()
        self.pool_manager.shutdown()

        # Clear memory cache
        self.clear_memory_cache()

        # Cancel remaining tasks
        with self._task_lock:
            for task in self.active_tasks.values():
                if not task._future.done():
                    task._future.cancel()
            self.active_tasks.clear()
            self.pending_submissions.clear()

        logger.info("✅ TaskMaster shutdown complete")


__all__: list[str] = [
    "Task",
    "TaskMaster",
    "HybridExecutor",
    "ResourceMonitor",
    "DynamicPoolManager",
]
