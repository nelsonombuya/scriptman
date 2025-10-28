"""
Unit tests for TaskManager class.
"""
import asyncio
import threading
import time
from concurrent.futures import Future
from unittest.mock import Mock, patch

import pytest

from scriptman.powers.tasks import TaskManager


class TestTaskManager:
    """Test cases for TaskManager class."""

    def test_singleton_pattern(self):
        """Test singleton pattern implementation."""
        # Reset singleton state
        TaskManager._TaskManager__instance = None
        TaskManager._TaskManager__initialized = False

        manager1 = TaskManager()
        manager2 = TaskManager()

        assert manager1 is manager2
        assert id(manager1) == id(manager2)

    def test_singleton_thread_safety(self):
        """Test singleton creation is thread-safe."""
        # Reset singleton state
        TaskManager._TaskManager__instance = None
        TaskManager._TaskManager__initialized = False

        managers = []
        errors = []

        def create_manager():
            try:
                manager = TaskManager()
                managers.append(manager)
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=create_manager)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join(timeout=1.0)

        # All managers should be the same instance
        assert len(errors) == 0
        assert len(managers) == 10
        assert all(manager is managers[0] for manager in managers)

    def test_initialization_state_persistence(self):
        """Test that initialization state persists across references."""
        # Reset singleton state
        TaskManager._TaskManager__instance = None
        TaskManager._TaskManager__initialized = False

        manager1 = TaskManager()
        manager2 = TaskManager()

        # Both should have same initialization state
        assert TaskManager._TaskManager__initialized is True
        assert manager1 is manager2

    def test_background_single_task(self, task_manager):
        """Test background method with single task."""
        def test_func(x, y):
            return x + y

        task = task_manager.background(test_func, 1, 2)

        assert isinstance(task, Future)
        result = task.await_result(timeout=1.0)
        assert result == 3

    def test_background_with_kwargs(self, task_manager):
        """Test background method with kwargs."""
        def test_func(x, y=0):
            return x + y

        task = task_manager.background(test_func, 1, y=2)

        result = task.await_result(timeout=1.0)
        assert result == 3

    def test_background_with_async_function(self, task_manager):
        """Test background method with async function."""
        async def async_func(x):
            await asyncio.sleep(0.01)
            return x * 2

        task = task_manager.background(async_func, 5)

        result = task.await_result(timeout=1.0)
        assert result == 10

    def test_background_after_shutdown(self, task_manager):
        """Test background method after shutdown."""
        task_manager.cleanup_all()

        task = task_manager.background(lambda: "test")

        # Should return empty task
        assert isinstance(task, Future)

    def test_multithread_parallel_execution(self, task_manager):
        """Test multithread method with parallel execution."""
        def test_func(x):
            time.sleep(0.01)
            return x * 2

        tasks = [
            (test_func, (1,), {}),
            (test_func, (2,), {}),
            (test_func, (3,), {}),
        ]

        batch = task_manager.multithread(tasks)

        assert len(batch) == 3
        results = batch.await_results()
        assert set(results) == {2, 4, 6}

    def test_multithread_with_progress_bar(self, task_manager):
        """Test multithread method with progress bar."""
        def test_func(x):
            return x

        tasks = [(test_func, (i,), {}) for i in range(3)]

        batch = task_manager.multithread(tasks, show_progress=True)

        assert len(batch) == 3
        results = batch.await_results()
        assert results == [0, 1, 2]

    def test_multithread_without_progress_bar(self, task_manager):
        """Test multithread method without progress bar."""
        def test_func(x):
            return x

        tasks = [(test_func, (i,), {}) for i in range(3)]

        batch = task_manager.multithread(tasks, show_progress=False)

        assert len(batch) == 3
        results = batch.await_results()
        assert results == [0, 1, 2]

    def test_multithread_empty_tasks_list(self, task_manager):
        """Test multithread method with empty tasks list."""
        with pytest.raises(ValueError, match="Tasks list cannot be empty"):
            task_manager.multithread([])

    def test_multithread_after_shutdown(self, task_manager):
        """Test multithread method after shutdown."""
        task_manager.cleanup_all()

        tasks = [(lambda: "test", (), {})]
        batch = task_manager.multithread(tasks)

        # Should return empty batch
        assert len(batch) == 0

    def test_parallel_alias(self, task_manager):
        """Test parallel method (alias for multithread)."""
        def test_func(x):
            return x

        tasks = [(test_func, (i,), {}) for i in range(3)]

        batch = task_manager.parallel(tasks)

        assert len(batch) == 3
        results = batch.await_results()
        assert results == [0, 1, 2]

    def test_race_first_successful(self, task_manager):
        """Test race method returns first successful result."""
        def fast_func():
            return "fast"

        def slow_func():
            time.sleep(0.1)
            return "slow"

        tasks = [
            (fast_func, (), {}),
            (slow_func, (), {}),
        ]

        winner = task_manager.race(tasks)

        result = winner.await_result(timeout=1.0)
        assert result == "fast"

    def test_race_with_timeout(self, task_manager):
        """Test race method with timeout."""
        def slow_func():
            time.sleep(0.1)
            return "slow"

        tasks = [(slow_func, (), {})]

        with pytest.raises(Exception):  # TimeoutError or similar
            winner = task_manager.race(tasks, timeout=0.05)
            winner.await_result()

    def test_race_with_preferred_task_idx(self, task_manager):
        """Test race method with preferred task index."""
        def failing_func():
            raise ValueError("failure")

        def success_func():
            return "success"

        tasks = [
            (failing_func, (), {}),
            (success_func, (), {}),
        ]

        winner = task_manager.race(tasks, preferred_task_idx=1)

        result = winner.await_result(timeout=1.0)
        assert result == "success"

    def test_race_empty_tasks_list(self, task_manager):
        """Test race method with empty tasks list."""
        with pytest.raises(ValueError, match="Tasks list cannot be empty"):
            task_manager.race([])

    def test_race_after_shutdown(self, task_manager):
        """Test race method after shutdown."""
        task_manager.cleanup_all()

        tasks = [(lambda: "test", (), {})]
        winner = task_manager.race(tasks)

        # Should return empty task
        assert isinstance(winner, Future)

    def test_await_async_static_method(self):
        """Test await_async static method."""
        async def async_func(x):
            await asyncio.sleep(0.01)
            return x * 2

        result = TaskManager.await_async(async_func(5))
        assert result == 10

    def test_await_async_with_existing_loop(self):
        """Test await_async with existing event loop."""
        async def async_func():
            return "existing_loop"

        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            result = TaskManager.await_async(async_func())
            assert result == "existing_loop"
        finally:
            loop.close()

    def test_set_resource_percentage_valid(self, task_manager):
        """Test set_resource_percentage with valid values."""
        TaskManager.set_resource_percentage(75.0)
        assert TaskManager.get_resource_percentage() == 75.0

        TaskManager.set_resource_percentage(25)
        assert TaskManager.get_resource_percentage() == 25.0

    def test_set_resource_percentage_invalid(self, task_manager):
        """Test set_resource_percentage with invalid values."""
        with pytest.raises(AssertionError, match="Resource percentage must be a number"):
            TaskManager.set_resource_percentage("invalid")  # type: ignore

        with pytest.raises(AssertionError, match="Resource percentage must be between 0.0 and 100.0"):
            TaskManager.set_resource_percentage(-1.0)

        with pytest.raises(AssertionError, match="Resource percentage must be between 0.0 and 100.0"):
            TaskManager.set_resource_percentage(101.0)

    def test_set_resource_percentage_updates_existing_executors(self, task_manager):
        """Test that set_resource_percentage updates existing executors."""
        # Access threads property to create executor
        executor = task_manager.threads

        # Update resource percentage
        TaskManager.set_resource_percentage(30.0)

        # Executor should be updated
        assert TaskManager.get_resource_percentage() == 30.0

    def test_get_resource_percentage(self, task_manager):
        """Test get_resource_percentage method."""
        percentage = TaskManager.get_resource_percentage()
        assert isinstance(percentage, float)
        assert 0.0 <= percentage <= 100.0

    def test_threads_property_lazy_initialization(self, task_manager):
        """Test threads property lazy initialization."""
        # Should not create executor initially
        assert task_manager._TaskManager__thread_executor is None

        # Should create executor on access
        executor = task_manager.threads
        assert executor is not None
        assert task_manager._TaskManager__thread_executor is executor

    def test_process_property_not_implemented(self, task_manager):
        """Test process property raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="ProcessExecutor not yet implemented"):
            _ = task_manager.process

    def test_asynchronous_property_not_implemented(self, task_manager):
        """Test asynchronous property raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="AsynchronousExecutor not yet implemented"):
            _ = task_manager.asynchronous

    def test_global_monitoring_startup(self, task_manager):
        """Test global monitoring thread startup."""
        # Access threads property to trigger monitoring
        executor = task_manager.threads

        # Monitoring should be active
        assert task_manager._TaskManager__monitoring is True
        assert task_manager._TaskManager__global_monitor_thread is not None
        assert task_manager._TaskManager__global_monitor_thread.is_alive()

    def test_global_monitoring_cleanup(self, task_manager):
        """Test global monitoring cleanup."""
        # Access threads property to start monitoring
        executor = task_manager.threads

        # Cleanup should stop monitoring
        task_manager.cleanup_all()

        assert task_manager._TaskManager__monitoring is False
        assert task_manager._TaskManager__is_shutdown is True

    def test_cleanup_all_with_wait(self, task_manager):
        """Test cleanup_all with wait=True."""
        # Submit a task
        task = task_manager.background(lambda: "test")

        # Cleanup should wait for task completion
        task_manager.cleanup_all(wait=True)

        assert task_manager._TaskManager__is_shutdown is True
        assert task.await_result() == "test"

    def test_cleanup_all_with_timeout(self, task_manager):
        """Test cleanup_all with timeout."""
        # Submit a slow task
        task = task_manager.background(lambda: time.sleep(0.1) or "slow")

        # Cleanup with short timeout
        task_manager.cleanup_all(wait=True, timeout=0.05)

        assert task_manager._TaskManager__is_shutdown is True

    def test_context_manager_support(self):
        """Test context manager support."""
        # Reset singleton state
        TaskManager._TaskManager__instance = None
        TaskManager._TaskManager__initialized = False

        with TaskManager() as manager:
            assert isinstance(manager, TaskManager)
            assert manager._TaskManager__is_shutdown is False

        # Should be cleaned up after context exit
        assert manager._TaskManager__is_shutdown is True

    def test_get_global_status(self, task_manager):
        """Test get_global_status method."""
        status = task_manager.get_global_status()

        assert isinstance(status, dict)
        assert "monitoring_active" in status
        assert "threads" in status
        assert "process" in status
        assert "asynchronous" in status

    def test_get_global_status_with_active_executors(self, task_manager):
        """Test get_global_status with active executors."""
        # Access threads property to create executor
        executor = task_manager.threads

        status = task_manager.get_global_status()

        assert status["monitoring_active"] is True
        assert status["threads"]["active"] is True
        assert status["threads"]["workers"] > 0

    def test_get_global_status_with_inactive_executors(self, task_manager):
        """Test get_global_status with inactive executors."""
        status = task_manager.get_global_status()

        assert status["monitoring_active"] is False
        assert status["threads"]["active"] is False
        assert status["threads"]["workers"] == 0

    def test_get_global_status_error_handling(self, task_manager):
        """Test get_global_status error handling."""
        # Mock an error in status calculation
        with patch.object(task_manager, '_TaskManager__get_executor_status', side_effect=Exception("Test error")):
            status = task_manager.get_global_status()

            assert "error" in status
            assert status["error"] == "Test error"

    def test_garbage_collection_cleanup(self, task_manager):
        """Test cleanup during garbage collection."""
        # Submit a task
        task = task_manager.background(lambda: "test")

        # Delete manager (should trigger __del__)
        del task_manager

        # Task should still complete
        result = task.await_result(timeout=1.0)
        assert result == "test"

    def test_operations_after_shutdown(self, task_manager):
        """Test operations after shutdown."""
        task_manager.cleanup_all()

        # All operations should handle shutdown gracefully
        task = task_manager.background(lambda: "test")
        batch = task_manager.multithread([(lambda: "test", (), {})])
        winner = task_manager.race([(lambda: "test", (), {})])

        # Should not raise exceptions
        assert isinstance(task, Future)
        assert len(batch) == 0
        assert isinstance(winner, Future)

    def test_multiple_executor_lifecycle(self, task_manager):
        """Test multiple executor lifecycle sequences."""
        # Create and use executor
        executor1 = task_manager.threads
        task1 = task_manager.background(lambda: "task1")
        result1 = task1.await_result(timeout=1.0)
        assert result1 == "task1"

        # Cleanup and recreate
        task_manager.cleanup_all()

        # Reset singleton state for new manager
        TaskManager._TaskManager__instance = None
        TaskManager._TaskManager__initialized = False

        manager2 = TaskManager()
        executor2 = manager2.threads
        task2 = manager2.background(lambda: "task2")
        result2 = task2.await_result(timeout=1.0)
        assert result2 == "task2"

        # Cleanup
        manager2.cleanup_all()
