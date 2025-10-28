"""
Unit tests for ThreadExecutor class.
"""
import asyncio
import time
from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import Mock, patch

import pytest

from scriptman.powers.tasks._thread_executor import ThreadExecutor


class TestThreadExecutor:
    """Test cases for ThreadExecutor class."""

    def test_default_initialization(self):
        """Test ThreadExecutor default initialization."""
        executor = ThreadExecutor()

        assert executor._max_workers > 0
        assert executor._idle_timeout == 30
        assert executor.is_shutdown is False
        assert executor.is_idle is False  # Just created, not idle yet
        assert executor.last_activity_time > 0

    def test_custom_initialization(self):
        """Test ThreadExecutor with custom parameters."""
        executor = ThreadExecutor(max_workers=5, idle_timeout=10)

        assert executor._max_workers == 5
        assert executor._idle_timeout == 10
        assert executor.is_shutdown is False

    def test_optimal_worker_calculation_with_psutil(self):
        """Test optimal worker calculation with psutil available."""
        with patch('scriptman.powers.tasks._thread_executor.cpu_count', return_value=8), \
             patch('scriptman.powers.tasks._thread_executor.virtual_memory') as mock_memory:

            mock_memory.return_value.total = 16 * 1024**3  # 16GB

            executor = ThreadExecutor()

            # Should calculate based on CPU and memory
            assert executor._max_workers > 0
            assert executor._max_workers <= 20  # Capped at 20

    def test_optimal_worker_calculation_without_psutil(self):
        """Test optimal worker calculation fallback without psutil."""
        with patch('scriptman.powers.tasks._thread_executor.cpu_count', return_value=8), \
             patch('scriptman.powers.tasks._thread_executor.virtual_memory', side_effect=ImportError):

            executor = ThreadExecutor()

            # Should fall back to default
            assert executor._max_workers == 4

    def test_submit_task_regular_function(self):
        """Test submitting regular function."""
        executor = ThreadExecutor(max_workers=2)

        def test_func(x, y):
            return x + y

        future = executor.submit_task(test_func, 1, 2)

        assert isinstance(future, Future)
        result = future.result(timeout=1.0)
        assert result == 3

    def test_submit_task_with_kwargs(self):
        """Test submitting function with kwargs."""
        executor = ThreadExecutor(max_workers=2)

        def test_func(x, y=0):
            return x + y

        future = executor.submit_task(test_func, 1, y=2)

        result = future.result(timeout=1.0)
        assert result == 3

    def test_submit_task_async_function(self):
        """Test submitting async function."""
        executor = ThreadExecutor(max_workers=2)

        async def async_func(x):
            await asyncio.sleep(0.01)
            return x * 2

        future = executor.submit_task(async_func, 5)

        result = future.result(timeout=1.0)
        assert result == 10

    def test_submit_task_after_shutdown(self):
        """Test submitting task after shutdown raises RuntimeError."""
        executor = ThreadExecutor(max_workers=2)
        executor.cleanup()

        with pytest.raises(RuntimeError, match="ThreadExecutor is shutdown"):
            executor.submit_task(lambda: "test")

    def test_submit_task_updates_activity_time(self):
        """Test that submitting task updates activity time."""
        executor = ThreadExecutor(max_workers=2, idle_timeout=1)

        initial_time = executor.last_activity_time
        time.sleep(0.01)  # Small delay

        executor.submit_task(lambda: "test")

        assert executor.last_activity_time > initial_time

    def test_submit_task_increments_active_count(self):
        """Test that submitting task increments active count."""
        executor = ThreadExecutor(max_workers=2)

        # Access private attribute for testing
        initial_count = executor._ThreadExecutor__active_task_count

        future = executor.submit_task(lambda: "test")

        # Count should be incremented
        assert executor._ThreadExecutor__active_task_count > initial_count

        # Wait for completion
        future.result(timeout=1.0)

    def test_cleanup_with_wait(self):
        """Test cleanup with wait=True."""
        executor = ThreadExecutor(max_workers=2)

        # Submit a task
        future = executor.submit_task(lambda: "test")

        # Cleanup should wait for task completion
        executor.cleanup(wait=True)

        assert executor.is_shutdown is True
        assert future.result() == "test"

    def test_cleanup_without_wait(self):
        """Test cleanup with wait=False."""
        executor = ThreadExecutor(max_workers=2)

        # Submit a slow task
        future = executor.submit_task(lambda: time.sleep(0.1) or "slow")

        # Cleanup should not wait
        executor.cleanup(wait=False)

        assert executor.is_shutdown is True

    def test_cleanup_with_timeout(self):
        """Test cleanup with timeout."""
        executor = ThreadExecutor(max_workers=2)

        # Submit a slow task
        future = executor.submit_task(lambda: time.sleep(0.1) or "slow")

        # Cleanup with short timeout
        executor.cleanup(wait=True, timeout=0.05)

        assert executor.is_shutdown is True

    def test_double_cleanup_idempotent(self):
        """Test that double cleanup is idempotent."""
        executor = ThreadExecutor(max_workers=2)

        executor.cleanup()
        assert executor.is_shutdown is True

        # Second cleanup should not raise exception
        executor.cleanup()
        assert executor.is_shutdown is True

    def test_is_idle_detection(self):
        """Test idle detection."""
        executor = ThreadExecutor(max_workers=2, idle_timeout=1)

        # Should not be idle immediately after creation
        assert executor.is_idle is False

        # Wait for idle timeout
        time.sleep(1.1)
        assert executor.is_idle is True

    def test_is_idle_after_shutdown(self):
        """Test that shutdown executor is considered idle."""
        executor = ThreadExecutor(max_workers=2)
        executor.cleanup()

        assert executor.is_idle is True

    def test_update_resource_limits(self):
        """Test updating resource limits."""
        executor = ThreadExecutor(max_workers=2)

        executor.update_resource_limits(5)

        assert executor._max_workers == 5

    def test_update_resource_limits_after_shutdown(self):
        """Test updating resource limits after shutdown."""
        executor = ThreadExecutor(max_workers=2)
        executor.cleanup()

        # Should not raise exception but also not update
        executor.update_resource_limits(5)

        assert executor.is_shutdown is True

    def test_get_debug_info(self):
        """Test get_debug_info method."""
        executor = ThreadExecutor(max_workers=3, idle_timeout=10)

        debug_info = executor.get_debug_info()

        assert isinstance(debug_info, dict)
        assert debug_info['is_shutdown'] is False
        assert debug_info['max_workers'] == 3
        assert debug_info['active_tasks'] == 0
        assert 'last_activity' in debug_info
        assert 'idle_duration' in debug_info
        assert 'executor_exists' in debug_info

    def test_lazy_executor_initialization(self):
        """Test that executor is created lazily."""
        executor = ThreadExecutor(max_workers=2)

        # Executor should not exist initially
        assert executor._ThreadExecutor__executor is None

        # Should be created on first task submission
        future = executor.submit_task(lambda: "test")
        assert executor._ThreadExecutor__executor is not None

        future.result(timeout=1.0)

    def test_executor_recreation_after_shutdown(self):
        """Test executor recreation after internal shutdown."""
        executor = ThreadExecutor(max_workers=2)

        # Submit task to create executor
        future1 = executor.submit_task(lambda: "test1")
        future1.result(timeout=1.0)

        # Manually shutdown internal executor
        executor._ThreadExecutor__executor.shutdown()

        # Should recreate executor on next submission
        future2 = executor.submit_task(lambda: "test2")
        assert executor._ThreadExecutor__executor is not None
        assert not executor._ThreadExecutor__executor._shutdown

        future2.result(timeout=1.0)

    def test_submit_task_during_resource_update(self):
        """Test task submission during resource limit update."""
        executor = ThreadExecutor(max_workers=2)

        # Start resource update in thread
        import threading
        update_thread = threading.Thread(target=executor.update_resource_limits, args=(5,))
        update_thread.start()

        # Submit task concurrently
        future = executor.submit_task(lambda: "test")

        update_thread.join(timeout=1.0)
        result = future.result(timeout=1.0)

        assert result == "test"
        assert executor._max_workers == 5

    def test_await_async_static_method(self):
        """Test await_async static method."""
        async def async_func(x):
            await asyncio.sleep(0.01)
            return x * 2

        result = ThreadExecutor.await_async(async_func(5))
        assert result == 10

    def test_await_async_with_existing_loop(self):
        """Test await_async with existing event loop."""
        async def async_func():
            return "existing_loop"

        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            result = ThreadExecutor.await_async(async_func())
            assert result == "existing_loop"
        finally:
            loop.close()

    def test_await_async_with_no_loop(self):
        """Test await_async with no current event loop."""
        async def async_func():
            return "no_loop"

        # Ensure no current loop
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            pass  # No current loop, which is what we want

        result = ThreadExecutor.await_async(async_func())
        assert result == "no_loop"

    def test_garbage_collection_cleanup(self):
        """Test cleanup during garbage collection."""
        executor = ThreadExecutor(max_workers=2)

        # Submit a task
        future = executor.submit_task(lambda: "test")

        # Delete executor (should trigger __del__)
        del executor

        # Task should still complete
        result = future.result(timeout=1.0)
        assert result == "test"

    def test_thread_safety(self):
        """Test thread safety of executor operations."""
        executor = ThreadExecutor(max_workers=4)

        import threading
        import queue

        results = queue.Queue()
        errors = queue.Queue()

        def worker(worker_id):
            try:
                future = executor.submit_task(lambda: f"worker_{worker_id}")
                result = future.result(timeout=1.0)
                results.put(result)
            except Exception as e:
                errors.put(e)

        # Start multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join(timeout=2.0)

        # Check results
        assert errors.empty(), f"Errors occurred: {list(errors.queue)}"
        assert results.qsize() == 10

        # Cleanup
        executor.cleanup()
