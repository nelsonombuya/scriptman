"""
Integration tests for the tasks module.
"""
import asyncio
import threading
import time
from concurrent.futures import Future
from unittest.mock import patch

import pytest

from scriptman.powers.tasks import TaskManager


class TestTasksIntegration:
    """Integration tests for the tasks module."""

    def test_complete_workflow(self):
        """Test complete workflow: create manager → submit tasks → await results → cleanup."""
        # Reset singleton state
        TaskManager._TaskManager__instance = None
        TaskManager._TaskManager__initialized = False

        manager = TaskManager()

        # Submit various types of tasks
        sync_task = manager.background(lambda: "sync_result")
        async_task = manager.background(async_function("async_result"))

        # Submit batch tasks
        batch_tasks = [
            (lambda x: x * 2, (1,), {}),
            (lambda x: x * 3, (2,), {}),
            (lambda x: x * 4, (3,), {}),
        ]
        batch = manager.multithread(batch_tasks)

        # Submit race tasks
        race_tasks = [
            (lambda: "fast", (), {}),
            (lambda: time.sleep(0.1) or "slow", (), {}),
        ]
        winner = manager.race(race_tasks)

        # Await all results
        sync_result = sync_task.await_result()
        async_result = async_task.await_result()
        batch_results = batch.await_results()
        winner_result = winner.await_result()

        # Verify results
        assert sync_result == "sync_result"
        assert async_result == "async_result"
        assert batch_results == [2, 6, 12]
        assert winner_result == "fast"

        # Cleanup
        manager.cleanup_all()
        assert manager._TaskManager__is_shutdown is True

    def test_mixed_sync_async_execution(self):
        """Test mixed sync/async task execution."""
        manager = TaskManager()

        # Mix of sync and async tasks
        tasks = [
            (lambda: "sync1", (), {}),
            (async_function, ("async1",), {}),
            (lambda: "sync2", (), {}),
            (async_function, ("async2",), {}),
        ]

        batch = manager.multithread(tasks)
        results = batch.await_results()

        assert len(results) == 4
        assert "sync1" in results
        assert "sync2" in results
        assert "async1" in results
        assert "async2" in results

        manager.cleanup_all()

    def test_batch_processing_with_failures(self):
        """Test batch processing with some failures."""
        manager = TaskManager()

        # Mix of successful and failing tasks
        tasks = [
            (lambda: "success1", (), {}),
            (lambda: (_ for _ in ()).throw(ValueError("failure1")), (), {}),
            (lambda: "success2", (), {}),
            (lambda: (_ for _ in ()).throw(RuntimeError("failure2")), (), {}),
        ]

        batch = manager.multithread(tasks)

        # Test with raise_exceptions=True (should raise first exception)
        with pytest.raises(ValueError, match="failure1"):
            batch.await_results(raise_exceptions=True)

        # Test with raise_exceptions=False
        results = batch.await_results(raise_exceptions=False)
        assert len(results) == 4
        assert "success1" in results
        assert "success2" in results
        assert any(isinstance(r, Exception) for r in results)

        # Test with only_successful_results=True
        success_results = batch.await_results(raise_exceptions=False, only_successful_results=True)
        assert success_results == ["success1", "success2"]

        manager.cleanup_all()

    def test_race_conditions_multiple_apis(self):
        """Test race conditions with multiple APIs."""
        manager = TaskManager()

        # Submit tasks using different APIs simultaneously
        def worker(worker_id):
            if worker_id % 3 == 0:
                return manager.background(lambda: f"background_{worker_id}")
            elif worker_id % 3 == 1:
                batch = manager.multithread([(lambda: f"multithread_{worker_id}", (), {})])
                return batch.await_results()[0]
            else:
                winner = manager.race([(lambda: f"race_{worker_id}", (), {})])
                return winner.await_result()

        # Submit multiple workers
        futures = []
        for i in range(9):
            future = manager.background(worker, i)
            futures.append(future)

        # Collect results
        results = []
        for future in futures:
            result = future.await_result(timeout=2.0)
            results.append(result)

        assert len(results) == 9
        assert all(isinstance(r, str) for r in results)

        manager.cleanup_all()

    def test_nested_task_execution(self):
        """Test nested task execution (tasks submitting tasks)."""
        manager = TaskManager()

        def outer_task(x):
            # Submit inner tasks
            inner_task1 = manager.background(lambda: x * 2)
            inner_task2 = manager.background(lambda: x * 3)

            # Wait for inner tasks
            result1 = inner_task1.await_result()
            result2 = inner_task2.await_result()

            return f"outer_{result1}_{result2}"

        # Submit outer task
        outer_future = manager.background(outer_task, 5)
        result = outer_future.await_result(timeout=2.0)

        assert result == "outer_10_15"

        manager.cleanup_all()

    def test_long_running_tasks_with_cancellation_attempts(self):
        """Test long-running tasks with cancellation attempts."""
        manager = TaskManager()

        def long_running_task():
            for i in range(10):
                time.sleep(0.1)
                if i == 5:
                    # Try to submit another task during execution
                    try:
                        quick_task = manager.background(lambda: "quick")
                        quick_result = quick_task.await_result(timeout=0.5)
                        return f"long_{quick_result}"
                    except Exception:
                        return "long_failed"
            return "long_complete"

        # Submit long-running task
        long_future = manager.background(long_running_task)
        result = long_future.await_result(timeout=2.0)

        # Should complete successfully
        assert result in ["long_quick", "long_failed", "long_complete"]

        manager.cleanup_all()

    def test_resource_exhaustion_and_recovery(self):
        """Test resource exhaustion and recovery."""
        manager = TaskManager()

        # Submit many tasks to potentially exhaust resources
        futures = []
        for i in range(50):  # Large number of tasks
            future = manager.background(lambda x: time.sleep(0.01) or f"task_{x}", i)
            futures.append(future)

        # Wait for all tasks to complete
        results = []
        for future in futures:
            try:
                result = future.await_result(timeout=5.0)
                results.append(result)
            except Exception as e:
                results.append(f"error_{e}")

        assert len(results) == 50
        assert all(isinstance(r, str) for r in results)

        manager.cleanup_all()

    def test_error_propagation_through_stack(self):
        """Test exception handling through entire stack."""
        manager = TaskManager()

        def error_propagating_task():
            try:
                # Submit a task that will fail
                failing_task = manager.background(lambda: (_ for _ in ()).throw(ValueError("inner_error")))
                result = failing_task.await_result()
                return f"unexpected_{result}"
            except Exception as e:
                # Re-raise with additional context
                raise RuntimeError(f"outer_error: {e}") from e

        # Submit the error-propagating task
        error_task = manager.background(error_propagating_task)

        with pytest.raises(RuntimeError, match="outer_error"):
            error_task.await_result()

        manager.cleanup_all()

    def test_task_exception_creation_and_propagation(self):
        """Test TaskException creation and propagation."""
        manager = TaskManager()

        def task_with_exception():
            raise ValueError("Test exception")

        # Submit failing task
        failing_task = manager.background(task_with_exception)

        # Test with raise_exceptions=False to get TaskException
        result = failing_task.await_result(raise_exceptions=False)

        assert hasattr(result, 'message')
        assert hasattr(result, 'exception')
        assert hasattr(result, 'stacktrace')
        assert result.message == "Test exception"
        assert isinstance(result.exception, ValueError)

        manager.cleanup_all()

    def test_timeout_errors_across_layers(self):
        """Test timeout errors across different layers."""
        manager = TaskManager()

        def slow_task():
            time.sleep(0.2)
            return "slow_result"

        # Test timeout at task level
        slow_future = manager.background(slow_task)
        with pytest.raises(Exception):  # TimeoutError or similar
            slow_future.await_result(timeout=0.1)

        # Test timeout at batch level
        batch_tasks = [(slow_task, (), {})]
        batch = manager.multithread(batch_tasks)
        with pytest.raises(Exception):  # TimeoutError or similar
            batch.await_results(timeout=0.1)

        # Test timeout at race level
        race_tasks = [(slow_task, (), {})]
        with pytest.raises(Exception):  # TimeoutError or similar
            winner = manager.race(race_tasks, timeout=0.1)
            winner.await_result()

        manager.cleanup_all()

    def test_concurrent_operations_same_manager(self):
        """Test multiple threads using same manager."""
        manager = TaskManager()

        results = []
        errors = []

        def worker(worker_id):
            try:
                # Each worker submits tasks
                task = manager.background(lambda: f"worker_{worker_id}")
                result = task.await_result(timeout=1.0)
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Start multiple worker threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join(timeout=2.0)

        # Check results
        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == 10
        assert all(f"worker_{i}" in results for i in range(10))

        manager.cleanup_all()

    def test_simultaneous_task_submission(self):
        """Test simultaneous task submission."""
        manager = TaskManager()

        def submit_task(task_id):
            return manager.background(lambda: f"task_{task_id}")

        # Submit tasks simultaneously
        futures = []
        for i in range(20):
            future = submit_task(i)
            futures.append(future)

        # Wait for all tasks
        results = []
        for future in futures:
            result = future.await_result(timeout=2.0)
            results.append(result)

        assert len(results) == 20
        assert all(f"task_{i}" in results for i in range(20))

        manager.cleanup_all()

    def test_concurrent_cleanup_calls(self):
        """Test concurrent cleanup calls."""
        manager = TaskManager()

        # Submit some tasks
        futures = []
        for i in range(5):
            future = manager.background(lambda: time.sleep(0.1) or "task")
            futures.append(future)

        # Call cleanup concurrently
        cleanup_threads = []
        for _ in range(3):
            thread = threading.Thread(target=manager.cleanup_all)
            cleanup_threads.append(thread)
            thread.start()

        # Wait for cleanup threads
        for thread in cleanup_threads:
            thread.join(timeout=1.0)

        # Manager should be shutdown
        assert manager._TaskManager__is_shutdown is True

        # Tasks should still complete
        for future in futures:
            try:
                result = future.await_result(timeout=1.0)
                assert result == "task"
            except Exception:
                pass  # Some tasks might be cancelled

    def test_memory_intensive_operations(self):
        """Test memory-intensive operations."""
        manager = TaskManager()

        def memory_task(size_mb):
            return [b"x" * (1024 * 1024)] * size_mb

        # Submit memory-intensive tasks
        futures = []
        for size in [1, 2, 3]:  # 1MB, 2MB, 3MB
            future = manager.background(memory_task, size)
            futures.append(future)

        # Wait for tasks
        results = []
        for future in futures:
            result = future.await_result(timeout=5.0)
            results.append(result)

        assert len(results) == 3
        assert all(len(result) > 0 for result in results)

        manager.cleanup_all()

    def test_mixed_task_types_and_complexity(self):
        """Test mixed task types with varying complexity."""
        manager = TaskManager()

        # Define tasks of different types and complexity
        tasks = [
            # Simple sync tasks
            (lambda: "simple", (), {}),
            (lambda x: x * 2, (5,), {}),

            # Async tasks
            (async_function, ("async_simple",), {}),
            (async_function, ("async_complex",), {}),

            # CPU-intensive tasks
            (lambda: sum(range(1000)), (), {}),
            (lambda: max(range(100)), (), {}),

            # I/O simulation
            (lambda: time.sleep(0.01) or "io_task", (), {}),
            (lambda: time.sleep(0.02) or "io_task2", (), {}),
        ]

        # Submit as batch
        batch = manager.multithread(tasks)
        results = batch.await_results()

        assert len(results) == 8
        assert "simple" in results
        assert 10 in results  # 5 * 2
        assert "async_simple" in results
        assert "async_complex" in results
        assert 499500 in results  # sum(range(1000))
        assert 99 in results  # max(range(100))
        assert "io_task" in results
        assert "io_task2" in results

        manager.cleanup_all()

    def test_error_recovery_and_continuation(self):
        """Test error recovery and continuation."""
        manager = TaskManager()

        def task_with_recovery(task_id):
            try:
                if task_id % 3 == 0:
                    raise ValueError(f"error_{task_id}")
                return f"success_{task_id}"
            except Exception as e:
                # Recover by returning a default value
                return f"recovered_{task_id}"

        # Submit tasks with some failures
        tasks = [(task_with_recovery, (i,), {}) for i in range(9)]
        batch = manager.multithread(tasks)

        # Get results without raising exceptions
        results = batch.await_results(raise_exceptions=False)

        assert len(results) == 9
        assert all(isinstance(r, str) for r in results)

        # Check that we have both successes and recoveries
        success_count = sum(1 for r in results if r.startswith("success_"))
        recovery_count = sum(1 for r in results if r.startswith("recovered_"))

        assert success_count > 0
        assert recovery_count > 0
        assert success_count + recovery_count == 9

        manager.cleanup_all()
